from datetime import datetime

class BucketCluster(object):
    '''
    classdocs
    '''
    def __init__(self, name, id_cluster):
        '''
        Constructor
        '''

        self.bucket_name = name
        self.id_cluster = id_cluster
        self.cdate = datetime.now()
        self.udate = datetime.now()
 
    def getDict(self):
        d = dict()
        d['id'] = self.id
        d['bucket_name'] = self.bucket_name
        d['id_cluster'] = self.id_cluster
        d['create_date'] = self.create_date.isoformat('-')
        d['last_update'] = self.last_update.isoformat('-')
        return d

    def __repr__(self):
        return "<BucketCluster(%s, %s, %s, %s, %s)>" % (self.id, self.bucket_name, self.id_cluster, self.cdate, self.udate)