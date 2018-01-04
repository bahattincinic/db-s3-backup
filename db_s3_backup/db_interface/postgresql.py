import subprocess

from .base import BaseDump


class PostgreSQLDump(BaseDump):

    def dump(self, config, s3_bucket, s3_bucket_key_name, filepath,
             verbose=False, upload_callback=None):
        sqldump_cmd = [
            'PGPASSWORD="%s"' % config['PASSWORD'],
            'pg_dump',
            '-d', config['NAME'],
            '-h', config['HOST'],
            '-p', config['PORT'],
            '-u', config['USER'],
            '> ', filepath
        ]

        if verbose:
            print('Dumping PostgreSQL database: {database} to file {filepath}'.format(
                database=config['NAME'], filepath=filepath))

        process = subprocess.Popen(sqldump_cmd, stdout=subprocess.PIPE)
        process.communicate()

        if upload_callback is not None:
            with open(filepath, 'w+') as f:
                upload_callback(f, s3_bucket, s3_bucket_key_name, verbose)
