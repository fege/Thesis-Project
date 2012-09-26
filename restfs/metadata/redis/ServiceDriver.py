import tornado.options
from tornado.options import define, options
from sqlalchemy     import create_engine
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy     import Table, Column, Integer, String, MetaData, DateTime, Unicode
from sqlalchemy     import ForeignKey, UniqueConstraint
from restfs.objects.BucketOwner import BucketOwner
from restfs.objects.Cluster import Cluster
from restfs.objects.BucketCluster import BucketCluster


#Ultima classe, opera sul DB
class ServiceDriver():
    
    def __init__(self):
        #Handle more database
        driver = ""
        define("service_db_debug", default="False", help="")
        define("service_db_sid"     , default="./db/restfs.db", help="")
        define("service_db_type" , default="sqlite", help="")
        engine = ""
        
        debug = False
        if options.service_db_debug == "True":
            debug  = True
        
        if options.service_db_type == "mysql" or options.service_db_type == "pymysql" :
            define("service_db_host", default="", help="")
            define("service_db_port", default="", help="", type=int)
            define("service_db_user", default="", help="")
            define("service_db_pwd" , default="", help="")
            
            # read conf file again
            tornado.options.parse_config_file(options.conf)
                
            host   = options.service_db_host
            port   = options.service_db_port 
            sid    = options.service_db_sid 
            user   = options.service_db_user 
            passwd = options.service_db_pwd
            if options.service_db_type == "mysql":
                driver = 'mysql://%s:%s@%s:%s/%s' % (user, passwd, host, port, sid)
            else:
                driver = 'mysql+pymysql://%s:%s@%s:%s/%s' % (user, passwd, host, port, sid)
            engine = create_engine(driver,pool_size=20, max_overflow=30, pool_recycle=3600, echo=debug)
             
        if options.service_db_type == "sqlite":    
            tornado.options.parse_config_file(options.conf)
            driver = 'sqlite:///'+options.service_db_sid
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
                         Column('id', Integer, 
                                primary_key=True ),
                         Column('ip', Integer),
                         Column('location',Unicode(500)),
                         Column('capacity', Integer),
                         Column('load', Integer),
                         Column('traffic', Integer),
                         Column('status',String(255)),
                         Column('cdate', DateTime),
                         Column('udate', DateTime),
                         sqlite_autoincrement=True               
                         ) 
        
        #Mapping Table
        mapper(Cluster,tbl_cluster) 
        
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
        
        metadata.create_all(engine) 
        self.session =  sessionmaker(bind=engine)
       

    def getBucketListByOwner(self,uid):
        session = self.session()
        res = session.query(BucketOwner).filter(BucketOwner.uid==uid)
        
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
