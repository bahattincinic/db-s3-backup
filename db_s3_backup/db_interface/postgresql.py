import subprocess
import os

from .base import BaseDump


class PostgreSQLDump(BaseDump):

    def dump(self, config, s3_bucket, s3_bucket_key_name, filepath,
             verbose=False, upload_callback=None):
        sqldump_cmd = [
            'pg_dump',
            '-d', config['NAME'],
            '-h', config['HOST'],
            '-p', config['PORT'],
            '-U', config['USER']
        ]

        process = subprocess.Popen(
            sqldump_cmd, stdout=subprocess.PIPE,
            env=dict(os.environ, PGPASSWORD=config['PASSWORD'])
        )

        if verbose:
            print('Dumping PostgreSQL database: {database} to file {filepath}'.format(
                database=config['NAME'], filepath=filepath))

        with open(filepath, 'w+') as f:
            while True:
                buf = process.stdout.read(4096 * 1024)  # Read 4 MB
                if buf:
                    f.write(buf.decode("utf-8"))
                    if verbose:
                        print('- Written 4 MB')
                else:
                    break

            if verbose:
                print('+ Dump finished')

            if upload_callback is not None:
                upload_callback(f, s3_bucket, s3_bucket_key_name, verbose)
