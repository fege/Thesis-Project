import tornado.options
from tornado.options import define, options
from sqlalchemy     import create_engine
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy     import Table, Column, Integer, String, MetaData, DateTime, Boolean, Text, Unicode
from sqlalchemy     import ForeignKey, UniqueConstraint
from restfs.objects.BucketOwner import BucketOwner
from restfs.objects.Zone import Zone
from restfs.objects.BucketCluster import BucketCluster
from restfs.objects.UserLog import UserLog



#Ultima classe, opera sul DB
class ResourceDriver(object):
    
    def __init__(self):
        #Handle more database
        driver = ""
        define("resource_db_debug", default="False", help="")
        define("resource_db_sid"     , default="./db/restfs.db", help="")
        define("resource_db_type" , default="sqlite", help="")
        engine = ""
        
        # First read for debug setting
        tornado.options.parse_config_file(options.conf)
        debug = False
        if options.resource_db_debug == "True":
            debug  = True
        
        if options.resource_db_type == "mysql" or options.resource_db_type == "pymysql" :
            define("resource_db_host", default="", help="")
            define("resource_db_port", default="", help="", type=int)
            define("resource_db_user", default="", help="")
            define("resource_db_pwd" , default="", help="")
            
            # read conf file again
            tornado.options.parse_config_file(options.conf)
                
            host   = options.resource_db_host
            port   = options.resource_db_port 
            sid    = options.resource_db_sid 
            user   = options.resource_db_user 
            passwd = options.resource_db_pwd
            if options.resource_db_type == "mysql":
                driver = 'mysql://%s:%s@%s:%s/%s' % (user, passwd, host, port, sid)
            else:
                driver = 'mysql+pymysql://%s:%s@%s:%s/%s' % (user, passwd, host, port, sid)
            engine = create_engine(driver,pool_size=20, max_overflow=30, pool_recycle=3600, echo=debug)
             
        if options.resource_db_type == "sqlite":    
            tornado.options.parse_config_file(options.conf)
            driver = 'sqlite:///'+options.resource_db_sid
            engine = create_engine(driver, echo=debug)
      
        ##########################################################################
        # Start build tables
        ##########################################################################
        
        metadata = MetaData()
        
        #tbl_buckets_owner
        #####################################
        tbl_buckets_owner = Table('tbl_buckets_owner', metadata,
                         Column('id', Integer, 
                                primary_key=True ),
                         Column('uid', Integer),
                         Column('bucket_name',Unicode(500)),
                         Column('description', Unicode(5000)),
                         Column('status',Integer),
                         Column('cdate', DateTime),
                         Column('udate', DateTime),
                         UniqueConstraint('bucket_name',  name='tbl_bucket_name_uniq'),
                         sqlite_autoincrement=True               
                         ) 
        
        #Mapping Table
        mapper(BucketOwner,tbl_buckets_owner) 
        
        #tbl_cluster
        #####################################
        tbl_cluster = Table('tbl_cluster', metadata,
                         Column('id', Integer,  primary_key=True ),
                         Column('ip', String(16)),
                         Column('location',Unicode(500)),
                         Column('capacity', Integer),
                         Column('sysload', Integer),
                         Column('traffic', Integer),
                         Column('status',Integer),
                         Column('cdate', DateTime),
                         Column('udate', DateTime),
                         sqlite_autoincrement=True               
                         ) 
        
        #Mapping Table
        mapper(Zone,tbl_cluster) 
        
        # link_owner_cluster
        #######################################
        lnk_owner_cluster = Table('lnk_owner_cluster', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('bucket_name', Unicode(500),
                                  ForeignKey('tbl_buckets_owner.bucket_name', onupdate="CASCADE", ondelete="CASCADE")),
                          Column('id_cluster', Integer,
                                 ForeignKey('tbl_cluster.id', onupdate="CASCADE", ondelete="CASCADE")
                                 ),
                          Column('cdate', DateTime),
                          Column('udate', DateTime),
                          UniqueConstraint('bucket_name','id_cluster' , name='tbl_owner_cluster_uniq'),
                          sqlite_autoincrement=True
                          )
     
        #Mapping Table
        mapper(BucketCluster,lnk_owner_cluster )


        #tbl_user
        #####################################
        tbl_user = Table('tbl_user', metadata,
                         Column('id', Integer, 
                                primary_key=True ),
                         Column('name', Unicode(500)),
                         Column('surname',Unicode(500)),
                         sqlite_autoincrement=True               
                         ) 
        
        #Mapping Table
        mapper(UserLog,tbl_user) 

        metadata.create_all(engine) 
        self.session =  sessionmaker(bind=engine)
       
    
    def check_user(self,name,surname):
        session = self.session()
        res = session.query(UserLog).filter(UserLog.name==name).filter(UserLog.surname==surname).count()
        session.close()
        return res
    
    def getBucketListByOwner(self,uid):
        session = self.session()
        res = session.query(BucketOwner).filter(BucketOwner.uid==uid).all()
        
        session.close()
        return res
    
    def getCountBucketByOwner(self,uid):
        session = self.session()
        num = session.query(BucketOwner).filter(BucketOwner.uid==uid).count()
        session.close()

        return num

    
    def getRegionList(self):
        
        regions = ['EU','US','us-west-1','ap-southeast-1','ap-northeast-1']
        return regions
    
    
    def findBucket(self, name):
        
        session = self.session()
        res = session.query(BucketOwner).filter(BucketOwner.bucket_name == name).count()
        session.close()
        
        return res
    
    
    def findCluster(self,name):
        session = self.session()
        list = []
        res = session.query(Zone).join(BucketCluster).join(BucketOwner).filter(BucketOwner.bucket_name == name)
        for el in res.all():
            list.append(el.ip)  
        session.close()
        
        return list
    
    def setBucketStatus(self, name, status):

        session = self.session()
        bucket = session.query(BucketOwner).filter(BucketOwner.bucket_name == name).first()
        bucket.status= status
        session.flush()
        session.commit()
        session.close()

    
    
    def addBucket(self,bucket):
        session = self.session()
        session.add(bucket)
        session.commit()
        session.close()
    
    
    def removeBucket(self, name):
        session = self.session()
        bucket = session.query(BucketOwner).filter(BucketOwner.bucket_name == name).first()
        session.delete(bucket)
        session.commit()
        session.close()
