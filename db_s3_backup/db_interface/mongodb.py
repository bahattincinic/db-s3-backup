import subprocess

from .base import BaseDump


class MongoDBDump(BaseDump):

    def dump(self, config, s3_bucket, s3_bucket_key_name, filepath,
             verbose=False, upload_callback=None):
        sqldump_cmd = [
            'mongodump',
            '--db', config['NAME'],
            '--host', config['HOST'],
            '--port', config['PORT'],
            '--username', config['USER'],
            '--password', config['PASSWORD'],
        ]
        proc = subprocess.Popen(sqldump_cmd, stdout=subprocess.PIPE)

        if verbose:
            print('Dumping MondoDB database: {database} to file {filepath}'.format(
                database=config['NAME'], filepath=filepath))

        with open(filepath, 'w+') as f:
            while True:
                buf = proc.stdout.read(4096 * 1024)  # Read 4 MB
                if buf != '':
                    f.write(buf)
                    if verbose:
                        print('- Written 4 MB')
                else:
                    break

            if verbose:
                print('+ Dump finished')

            if upload_callback is not None:
                upload_callback(f, s3_bucket, s3_bucket_key_name, verbose)
