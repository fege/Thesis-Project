from xml.dom.minidom import parseString

class DataToXML(object):
    
    def __init__(self,data):
        self.xml = parseString(data)
        
    def getXML(self):
        return self.xml

class XmlToDict(object):
    
    def __init__(self,data):
        dom = parseString(data)
        self._dict = self.nodeToDic(dom)
        
    def getDict(self):
        return self._dict
        
    def getTextFromNode(self,node):
        """
        scans through all children of node and gathers the
        text. if node has non-text child-nodes, then
        NotTextNodeError is raised.
        """
        t = ""
        for n in node.childNodes:
            if n.nodeType == n.TEXT_NODE:
                t += n.nodeValue
            else:
                raise NotTextNodeError
        return t
    
    
    
    def nodeToDic(self,node):
        dic = {} 
        for n in node.childNodes:
            if n.nodeType != n.ELEMENT_NODE:
                continue
            if n.getAttribute("multiple") == "true":
                # node with multiple children:
                # put them in a list
                l = []
                for c in n.childNodes:
                    if c.nodeType != n.ELEMENT_NODE:
                        continue
                    l.append(self.nodeToDic(c))
                    dic.update({n.nodeName:l})
                continue        
            try:
                text = self.getTextFromNode(n)
            except NotTextNodeError:
                    # 'normal' node
                    if dic.has_key(n.nodeName):
                        if isinstance(dic[n.nodeName],list):
                            dic[n.nodeName].append(self.nodeToDic(n))
                        else:
                            el = dic[n.nodeName]
                            dic[n.nodeName] = [el]
                            dic[n.nodeName].append(self.nodeToDic(n))
                    else :
                        dic.update({n.nodeName:self.nodeToDic(n)})
                    continue 
                # text node
            dic.update({n.nodeName:text})
            continue
        return dic   



class DictToXml(object):
    """Class used to convert a python dictionary to XML"""
    def __init__(self, dict):
        """Create the XML and store it"""
        self.dict = dict
        self.xml  = self._convert_dict_to_xml(self.dict)
 
    def getXml(self):
        """Returns the XML"""
        return self.xml
    
    def prettify(self):
        return parseString(self.xml).toprettyxml()
 

 
    def _convert_dict_to_xml(self, dict):
        """Converts a dictionary to XML"""
        xml = ''
        for k,v in dict.items(): # iterate through the dictionary
            if self._is_dict(v): # the value is a dictionary
                xml += "<%s>" % (k)
                xml += self._convert_dict_to_xml(v) # recursively call itself
                xml += "</%s>" % (k)
            else: #value is not a dictionary
                if not self._is_list(v):
                    v = [v]
                for value in v:
                    if self._is_dict(value):
                        xml += "<%s>" % (k)
                        xml += self._convert_dict_to_xml(value) # recursively call itself
                        xml += "</%s>" % (k)
                    else:
                        xml += "<%s>%s</%s>" % (str(k),str(value),str(k))
        return xml
 
    def _is_dict(self, a):
        """Returns True if the item is a dictionary"""
        return type(a) == type({})
 
    def _is_list(self, a):
        """Returns True if the item is a list"""
        return type(a) == type([])

class NotTextNodeError(BaseException):
    pass