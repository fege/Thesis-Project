

TYPE_DIR   = 1
TYPE_FILE  = 2
TYPE_MOUNT = 3
TYPE_ROOT  = 4
                                #    UGO = User, Group, Other
CHMOD_TYPE_NONE       = '0'     #    ---     no permission
CHMOD_TYPE_EXEC       = '1'     #    --x     execution
CHMOD_TYPE_WRITE      = '2'     #    -w-     writing
CHMOD_TYPE_WRITE_EXEC = '3'     #    -wx     writing and execution
CHMOD_TYPE_READ       = '4'     #    r--     reading
CHMOD_TYPE_READ_EXEC  = '5'     #    r-x     reading and execution
CHMOD_TYPE_READ_WRITE = '6'     #    rw-     reading e writing
CHMOD_TYPE_ALL        = '7'     #    rwx     reading, writing, execution


ENOENT  = "-ENOENT"
EACCESS = "-EACCESS"


PROPERTY_OBJECT = "OBJECT"
PROPERTY_ACL    = "ACL"
PROPERTY_USER   = "USER"