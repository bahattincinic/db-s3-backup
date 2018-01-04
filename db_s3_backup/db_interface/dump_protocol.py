from exceptions import ValueError


class DumpProtocol(object):

    def dump(self, config, s3_bucket, s3_bucket_key_name,
             filepath, verbose=False, upload_callback=None):
        raise NotImplementedError('dump not implemented by {0}'.format(self))
