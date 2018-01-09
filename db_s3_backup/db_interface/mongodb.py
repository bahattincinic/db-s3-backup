import subprocess
import tarfile
import os

from .base import BaseDump


class MongoDBDump(BaseDump):

    def get_folder_name(self, filepath):
        return ('.').join(filepath.split('.')[:-1])

    def dump(self, config, s3_bucket, s3_bucket_key_name, filepath,
             verbose=False, upload_callback=None):
        folder_path = self.get_folder_name(filepath)
        sqldump_cmd = [
            'mongodump',
            '-d', config['NAME'],
            ('-host', config['HOST']) if config.get('HOST') else '',
            ('-port', config['PORT']) if config.get('PORT') else '',
            ('-u', config['USER']) if config.get('USERNAME') else '',
            ('--port', config['PASSWORD']) if config.get('PASSWORD') else '',
            '-o %s' % folder_path
        ]
        process = subprocess.Popen(
            sqldump_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.communicate()

        if verbose:
            print('Dumping MondoDB database: {database} to file {filepath}'.format(
                database=config['NAME'], filepath=filepath))

        with tarfile.open(filepath, "w:gz") as tar:
            tar.add(folder_path, arcname=os.path.basename(folder_path))

            if upload_callback is not None:
                upload_callback(tar, s3_bucket, s3_bucket_key_name, verbose)
