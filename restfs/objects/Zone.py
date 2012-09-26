from datetime import datetime

class Zone(object):
    '''
    classdocs
    '''
        
    
    def __init__(self, ip, location, capacity, sysload,
                 traffic, status):
        '''
        Constructor
        '''
        self.ip = ip
        self.location = location
        self.capacity = capacity
        self.sysload  = sysload
        self.traffic  = traffic
        self.status   = status
        self.cdate = datetime.now()
        self.udate = datetime.now()
    
    def getDict(self):
        d = dict()
        d['id'] = self.id
        d['ip'] = self.ip
        d['location'] = self.location
        d['sysload'] = self.sysload
        d['traffic'] = self.traffic
        d['status'] = self.status
        d['cdate'] = self.create_date.isoformat('-')
        d['udate'] = self.last_update.isoformat('-')
        return d
        
    def __repr__(self):
        return "<Zone(%s,%s,%s,%s,%s,%s,%s,%s,%s)>" % (self.id,self.ip,self.location,self.capacity,self.sysload,self.traffic,self.status,self.cdate,self.udate)

    
