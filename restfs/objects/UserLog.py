from datetime import datetime

class UserLog(object):
    '''
    classdocs
    '''
        
    
    def __init__(self, name, surname):
        '''
        Constructor
        '''
        self.name = name
        self.surname = surname
    
    def getDict(self):
        d = dict()
        d['id'] = self.id
        d['name'] = self.name
        d['surname'] = self.surname
        return d
        
    def __repr__(self):
        return "<UserLog(%s,%s,%s)>" % (self.id,self.name,self.surname)

    
