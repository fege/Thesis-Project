import datetime

class BucketStats():
    def __init__(self, num_object = 0, max_object=1000, used_space=0):
        self.num_object = num_object
        self.max_object = max_object
        self.used_space = used_space
        self.creation_date = datetime.datetime.now()
        self.last_update = ''
        self.num_letture = ''
        