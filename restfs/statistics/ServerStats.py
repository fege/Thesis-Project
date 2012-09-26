class ServerStats():
    
    def __init__(self,uid,totByte,byteOut,byteIn,outbound,inbound,size,numCluster,visitNumber=0,deleteList=[],numOperations=0):
        self.uid = uid
        self.totByte = totByte
        self.numCluster = numCluster
        self.visitNumber = visitNumber
        self.size = size
        self.outbound = outbound
        self.inbound = inbound
        self.byteIn = byteIn
        self.byteOut = byteOut
        self.deleteList = deleteList
        self.numOperations = numOperations        
    
    def setByDict(self,myDict):
        for key in myDict:
            setattr(self,key,myDict[key])
        
    def getDict(self):
        myDict = {}
        myDict["uid"] = self.uid
        myDict["totByte"] = self.totByte
        myDict["byteIn"] = self.byteIn
        myDict["byteOut"] = self.byteOut
        myDict["numCluster"] = self.numCluster
        myDict["visitNumber"] = self.visitNumber
        myDict["size"] = self.size
        myDict["outbound"] = self.outbound
        myDict["inbound"] = self.inbound
        myDict["deleteList"] = self.deleteList
        myDict["numOperations"] = self.numOperations
        return myDict