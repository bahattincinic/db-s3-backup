import subprocess

from .base import BaseDump


class MongoDBDump(BaseDump):

    def get_folder_name(self, filepath):
        return ('.').join(filepath.split('.')[:-1])

    def dump(self, config, s3_bucket, s3_bucket_key_name, filepath,
             verbose=False, upload_callback=None):
        folder_path = self.get_folder_name(filepath)
        sqldump_cmd = [
            'mongodump',
            '--db', config['NAME'],
            '--host', config['HOST'],
            '--port', config['PORT'],
            '--username', config['USER'],
            '--password', config['PASSWORD'],
            '-o %s' % folder_path
        ]
        subprocess.call(sqldump_cmd, stdout=subprocess.PIPE)

        if verbose:
            print('Dumping MondoDB database: {database} to file {filepath}'.format(
                database=config['NAME'], filepath=filepath))

        with tarfile.open(filepath, "w:gz") as tar:
            tar.add(folder_path, arcname=os.path.basename(folder_path))

            if upload_callback is not None:
                upload_callback(tar, s3_bucket, s3_bucket_key_name, verbose)
