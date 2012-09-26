'''
Created on 09/gen/2012

@author: fede
'''
import sys
import re
from optparse import OptionParser, Option, OptionValueError, IndentedHelpFormatter
from copy import copy
 ## Our modules
## Keep them in try/except block to 
## detect any syntax errors in there
from S3.Exceptions import *
from S3 import PkgInfo
from S3.S3 import S3
from S3.Config import Config
from S3.SortedDict import SortedDict
from S3.S3Uri import S3Uri
from S3 import Utils
from S3.Utils import *
from S3.Progress import Progress
from S3.CloudFront import Cmd as CfCmd

'''def check_uri(args):
    _re = re.compile("^s3://([^/]+)/?(.*)", re.IGNORECASE)
    match = _re.match(args)
    if not match:
        raise ValueError("%s: not a S3 URI" % args)
    groups = match.groups()
    bucket = groups[0]
    object = groups[1]
    return bucket,object
'''
class OptionMimeType(Option):
    def check_mimetype(option, opt, value):
        if re.compile("^[a-z0-9]+/[a-z0-9+\.-]+$", re.IGNORECASE).match(value):
            return value
        raise OptionValueError("option %s: invalid MIME-Type format: %r" % (opt, value))

class OptionS3ACL(Option):
    def check_s3acl(option, opt, value):
        permissions = ('read', 'write', 'read_acp', 'write_acp', 'full_control', 'all')
        try:
            permission, grantee = re.compile("^(\w+):(.+)$", re.IGNORECASE).match(value).groups()
            if not permission or not grantee:
                raise
            if permission in permissions:
                return { 'name' : grantee, 'permission' : permission.upper() }
            else:
                raise OptionValueError("option %s: invalid S3 ACL permission: %s (valid values: %s)" % 
                    (opt, permission, ", ".join(permissions)))
        except:
            raise OptionValueError("option %s: invalid S3 ACL format: %r" % (opt, value))

class OptionAll(OptionMimeType, OptionS3ACL):
   TYPE_CHECKER = copy(Option.TYPE_CHECKER)
   TYPE_CHECKER["mimetype"] = OptionMimeType.check_mimetype
   TYPE_CHECKER["s3acl"] = OptionS3ACL.check_s3acl
   TYPES = Option.TYPES + ("mimetype", "s3acl")

class MyHelpFormatter(IndentedHelpFormatter):
   def format_epilog(self, epilog):
       if epilog:
           return "\n" + epilog + "\n"
       else:
           return ""

def output(message):
    sys.stdout.write(message + "\n")
    
def cmd_bucket_create(args):
    #bucket, object = check_uri(args)
    print 'dentro al cmd create'
    s3 = S3(Config())
    for arg in args:
        uri = S3Uri(arg)
        if not uri.type == "s3" or not uri.has_bucket() or uri.has_object():
            raise ParameterError("Expecting S3 URI with just the bucket name set instead of '%s'" % arg)
        try:
            print 'dentro al try, provo il create'
            response = s3.bucket_create(uri.bucket(), Config().bucket_location)
            print 'ho la response'
            output(u"Bucket '%s' created" % uri.uri())
        except S3Error, e:
            if S3.codes.has_key(e.info["Code"]):
                error(S3.codes[e.info["Code"]] % uri.bucket())
                return
            else:
                raise

def cmd_bucket_delete():
    pass

def cmd_ls():
    pass

def cmd_buckets_list_all_all():
    pass

def cmd_object_put():
    pass

def cmd_object_get():
    pass

def cmd_object_del():
    pass

def get_commands_list():
    return [
    {"cmd":"mb", "label":"Make bucket", "param":"s3://BUCKET", "func":cmd_bucket_create, "argc":1},
    {"cmd":"rb", "label":"Remove bucket", "param":"s3://BUCKET", "func":cmd_bucket_delete, "argc":1},
    {"cmd":"ls", "label":"List objects or buckets", "param":"[s3://BUCKET[/PREFIX]]", "func":cmd_ls, "argc":0},
    {"cmd":"la", "label":"List all object in all buckets", "param":"", "func":cmd_buckets_list_all_all, "argc":0},
    {"cmd":"put", "label":"Put file into bucket", "param":"FILE [FILE...] s3://BUCKET[/PREFIX]", "func":cmd_object_put, "argc":2},
    {"cmd":"get", "label":"Get file from bucket", "param":"s3://BUCKET/OBJECT LOCAL_FILE", "func":cmd_object_get, "argc":1},
    {"cmd":"del", "label":"Delete file from bucket", "param":"s3://BUCKET/OBJECT", "func":cmd_object_del, "argc":1}
    ]

if __name__ == "__main__":

        
    if len(sys.argv) != 3:
        print "Field 1         : Operation command"
        print "Field 2         : Parameter"
        print "Example         : MB s3://mioBucket"
        print "Another Example : PUT s3://mioBucket/example.txt"
        exit()
    obj = None
 
    operation = str(sys.argv[1])
    parameter = str(sys.argv[2])
    optparser = OptionParser(option_class=OptionAll, formatter=MyHelpFormatter())
    (options, args) = optparser.parse_args()

    commands = {}
    command_list = get_commands_list()
    for cmd in command_list:
        if cmd.has_key('cmd'):
            commands[cmd['cmd']] =  cmd
    
    command = args.pop(0)
    cmd_func = commands[command]['func']
    
    try:
        cmd_func(args)
    except:
        print 'error'
        sys.exit(1)