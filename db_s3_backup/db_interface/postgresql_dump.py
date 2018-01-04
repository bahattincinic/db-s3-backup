import subprocess

from .dump_protocol import DumpProtocol


class PostgreSQLDump(DumpProtocol):

    def dump(self, config, s3_bucket, s3_bucket_key_name, filepath,
             verbose=False, upload_callback=None):
        sqldump_cmd = [
            'PGPASSWORD="%s"' % config['PASSWORD'],
            'pg_dump',
            '-d', config['NAME'],
            '-h', config['HOST'],
            '-p', config['PORT'],
            '-u', config['USER']
        ]
        proc = subprocess.Popen(sqldump_cmd, stdout=subprocess.PIPE)

        if verbose:
            print('Dumping PostgreSQL database: {database} to file {filepath}'.format(
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
