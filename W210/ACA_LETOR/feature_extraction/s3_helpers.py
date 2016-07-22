import boto3

class s3_helper:
    '''
    NOTE: local folder structure within offline folder mirrors folder in S3
    '''

    bucket = None

    def __init__(self, region='us-east-1', bucket_name='w210.data'):
        s3 = boto3.resource('s3', region)
        self.bucket = s3.Bucket(bucket_name)

    def download_feature_pickle(self, state):
        '''
        download feature pickle for a state
        assuming all feature pickle start with state abbreviation
        '''
        for o in self.bucket.objects.all():
            if o.key.startswith('feature/%s_' %state):
                self.bucket.download_file(o.key, o.key)
                print 'successfully download feature %s from S3' %o.key
                return o.key
        else:
            # no file found
            print 'no feature pickle found for state %s' %state
            return None

    def upload(self, key):
        '''
        upload plan rank for query clusters
        '''
        self.bucket.upload_file(key)

    def upload2(self, src, des):
        self.bucket.upload_file(src, des)

    def delete(self, key):
        for obj in self.bucket.objects.all():
            if obj.key == key: #'online/runtime_data_OR.pickle':
                obj.delete()
                return True
        else:
            print 'key %s not found' %key
            return False

    def set_public(self, key):
        '''
        '''
        for obj in bucket.objects.all():
            if obj.key == key: # 'feature/SD_18_26728.pickle':
                obj.Acl().put(ACL='public-read')
                return True
        else:
            print 'key %s not found' %key
            return False
