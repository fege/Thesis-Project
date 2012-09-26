import os.path
import tornado.options
from restfs.objects.User import User
from tornado.options import define, options

class AuthDriver(object):
    
    _USER_DB_PATH  = "/tmp/users.txt"
    _USER_DB_CACHE = dict()
    _USER_DB_INFO  = ""
    
    def __init__(self):
        define("auth_db_path",  default="/tmp/users.txt" , help="user storage")
        
        #Read Command line with new definitions
        tornado.options.parse_config_file(options.conf)
        
        #Check param .. force help
        
        self._USER_DB_PATH    = os.path.abspath(options.auth_db_path) 
        
        if not os.path.exists:
            exit("User Password File Not found")
        
        self._USER_DB_INFO = os.stat(self._USER_DB_PATH).st_mtime
        self._loadUserDB()           
          
    def getUser(self,user_name):
        db_mtime = os.stat(self._USER_DB_PATH).st_mtime
        if self._USER_DB_INFO != db_mtime :
            self._loadUserDB()
            self._USER_DB_INFO = db_mtime
        
        return self._USER_DB_CACHE.get(user_name,None)
              
              
          
    # must be syncronized !       
    def _loadUserDB(self):
        self._USER_DB_CACHE = dict()
        f = open(self._USER_DB_PATH, 'r')
        
        for line in f:
            line = line.split(":")
            if len(line) == 7:
                user = User(line[0],line[1],line[2],line[3],line[4],line[5],line[6])
                self._USER_DB_CACHE[line[1]] = user
