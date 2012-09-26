from datetime import datetime

class BucketOwner(object):
    '''
    classdocs
    '''
    def __init__(self,uid, bucket_name):
        '''
        Constructor
        '''

        self.uid = uid
        self.bucket_name = bucket_name
        self.description = ""
        self.status = 1
        self.cdate = datetime.now()
        self.udate = datetime.now()

    def getDict(self):
        d = dict()
        d['id'] = self.id
        d['uid'] = self.uid
        d['bucket_name'] = self.bucket_name
        d['description'] = self.description
        d['status'] = self.status
        d['cdate'] = self.create_date.isoformat('-')
        d['udate'] = self.last_update.isoformat('-')
        return d

    def __repr__(self):
        return "<BucketOwner(%s, %s, %s, %s, %s, %s, %s)>" % (self.id, self.uid, self.bucket_name, self.status, self.cdate, self.udate)