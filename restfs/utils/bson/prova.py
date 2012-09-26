'''
Created on 08/feb/2012

@author: fede
'''
from restfs.utils.bson import BSON
from restfs.utils.bson import Binary
from restfs.utils.bson import ObjectId
from restfs.utils.bson import Code
from restfs.utils.bson import DBRef
import unittest


BSON.encode({"test": u"hello world"})
print BSON.encode({})

class TestBSON(unittest.TestCase):
    def test_basic_encode(self):
            self.assertRaises(TypeError, BSON.encode, 100)
            self.assertRaises(TypeError, BSON.encode, "hello")
            self.assertRaises(TypeError, BSON.encode, None)
            self.assertRaises(TypeError, BSON.encode, [])
      
            self.assertEqual(BSON.encode({}), BSON("\x05\x00\x00\x00\x00"))
            self.assertEqual(BSON.encode({"test": u"hello world"}),
                             "\x1B\x00\x00\x00\x02\x74\x65\x73\x74\x00\x0C\x00\x00"
                             "\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F\x72\x6C\x64\x00"
                             "\x00")
            '''self.assertEqual(BSON.encode({u"mike": 100}),
                             "\x0F\x00\x00\x00\x10\x6D\x69\x6B\x65\x00\x64\x00\x00"
                             "\x00\x00")
            self.assertEqual(BSON.encode({"hello": 1.5}),
                             "\x14\x00\x00\x00\x01\x68\x65\x6C\x6C\x6F\x00\x00\x00"
                             "\x00\x00\x00\x00\xF8\x3F\x00")
            self.assertEqual(BSON.encode({"true": True}),
                             "\x0C\x00\x00\x00\x08\x74\x72\x75\x65\x00\x01\x00")
            self.assertEqual(BSON.encode({"false": False}),
                             "\x0D\x00\x00\x00\x08\x66\x61\x6C\x73\x65\x00\x00"
                             "\x00")
            self.assertEqual(BSON.encode({"empty": []}),
                             "\x11\x00\x00\x00\x04\x65\x6D\x70\x74\x79\x00\x05\x00"
                             "\x00\x00\x00\x00")
            self.assertEqual(BSON.encode({"none": {}}),
                             "\x10\x00\x00\x00\x03\x6E\x6F\x6E\x65\x00\x05\x00\x00"
                             "\x00\x00\x00")
            self.assertEqual(BSON.encode({"test": Binary("test", 0)}),
                             "\x14\x00\x00\x00\x05\x74\x65\x73\x74\x00\x04\x00\x00"
                             "\x00\x00\x74\x65\x73\x74\x00")
            self.assertEqual(BSON.encode({"test": Binary("test")}),
                             "\x18\x00\x00\x00\x05\x74\x65\x73\x74\x00\x08\x00\x00"
                             "\x00\x02\x04\x00\x00\x00\x74\x65\x73\x74\x00")
            self.assertEqual(BSON.encode({"test": Binary("test", 128)}),
                             "\x14\x00\x00\x00\x05\x74\x65\x73\x74\x00\x04\x00\x00"
                             "\x00\x80\x74\x65\x73\x74\x00")
            self.assertEqual(BSON.encode({"test": None}),
                             "\x0B\x00\x00\x00\x0A\x74\x65\x73\x74\x00\x00")
            self.assertEqual(BSON.encode({"$where": Code("test")}),
                             "\x1F\x00\x00\x00\x0F\x24\x77\x68\x65\x72\x65\x00\x12"
                             "\x00\x00\x00\x05\x00\x00\x00\x74\x65\x73\x74\x00\x05"
                             "\x00\x00\x00\x00\x00")
            a = ObjectId("\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B")
            self.assertEqual(BSON.encode({"oid": a}),
                             "\x16\x00\x00\x00\x07\x6F\x69\x64\x00\x00\x01\x02\x03"
                             "\x04\x05\x06\x07\x08\x09\x0A\x0B\x00")
            self.assertEqual(BSON.encode({"ref": DBRef("coll", a)}),
                             "\x2F\x00\x00\x00\x03ref\x00\x25\x00\x00\x00\x02$ref"
                             "\x00\x05\x00\x00\x00coll\x00\x07$id\x00\x00\x01\x02"
                             "\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x00\x00")'''
        
    def test_basic_decode(self):
        self.assertEqual({"test": u"hello world"},
                         BSON("\x1B\x00\x00\x00\x0E\x74\x65\x73\x74\x00\x0C"
                              "\x00\x00\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F"
                              "\x72\x6C\x64\x00\x00").decode())

if __name__ == "__main__":
    unittest.main()