'''
Created on 27/ott/2011

@author: fede
'''
import unittest

from restfs.backends.fs.MetadataDriver import MetadataDriver


class Test(unittest.TestCase):
    
    meta = MetadataDriver()


    def testGetBucketProperties(self):
        known_values = (('ciao','<Global><logging>None</logging><uid>manfred</uid><notification>False</notification><region>EU</region><logging_prefix></logging_prefix><logging_target></logging_target><versioning_mfa>\
        Disabled</versioning_mfa><acp>True</acp><space_used>0</space_used><objectCounter>0</objectCounter><policy>False</policy><versioning>None</versioning></Global>","<Global><logging>None</logging><uid>manfred</uid>\
        <notification>False</notification><region>EU</region><logging_prefix></logging_prefix><logging_target>\
        </logging_target><versioning_mfa>Disabled</versioning_mfa><acp>True</acp><space_used>0</space_used>\
        <objectCounter>0</objectCounter><policy>False</policy><versioning>None</versioning></Global>'), \
                        \
                        ('prova','<Global><logging>None</logging><uid>manfred</uid><notification>False</notification>\
                        <region>EU</region><logging_prefix></logging_prefix><logging_target></logging_target><versioning_mfa>\
                        Disabled</versioning_mfa><acp>None</acp><space_used>0</space_used><objectCounter>2\
                        </objectCounter><policy>False</policy><versioning>None</versioning></Global>'))
        
        for x,y in known_values:
            risultato  = MetadataDriver.getBucketProperties(self.meta,x)
            self.assertEqual(y,risultato)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()