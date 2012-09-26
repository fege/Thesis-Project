try:
    import cPickle as pickle
except:
    import pickle
 
class MetadataBase(object):    

    ##########################################################
    #     Internals
    ##########################################################   
    
    def _load(self,serialized):
        if not serialized:
            return None
        
        return pickle.loads(serialized)

    def _dumps(self,obj):       
        if not obj:
            return  None

        return pickle.dumps(obj) 