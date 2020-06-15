import boto3
from botocore.exceptions import ClientError
import logging, argparse, pathlib, re, os, sys
import pandas as pd

sys.path.append("../s3_helper")
from s3helper import S3helper

logging.basicConfig(filename='flowlog-panda.log', filemode='w', level=logging.INFO)

def main():

    # Initiate argument parser
    parser = argparse.ArgumentParser(description="Undelete S3 Objects")
    parser.add_argument("--account_profile", "-a", help="Provide an account profile name e.g. sg-ssd-com-lma")
    parser.add_argument("--bucket_name", "-b", help="The name of the bucket to process")
    parser.add_argument("--prefix", "-p", help="Used to select only those keys that begin with the specified prefix")
    parser.add_argument("--target_dir", "-t", help="Directory on the local machine to store the downloaded objects")
    parser.add_argument("--download_data", "-d", type=eval, choices=[True, False], default=False, help="Flag to determine if data should be pulled from S3. Can be True or False (default = False")
    parser.add_argument("--preserve_keys", "-k", type=eval, choices=[True, False], default=True, help="Flag to determine the keys paths should be preserved  (default = True")

    args = parser.parse_args()

    if not args.account_profile:
        logging.error("An aws profile must be provided. Run \"python flowlog-panda.py -h\"")
        print("An aws profile must be provided. Run \"python flowlog-panda.py -h\"")
        return
    
    if not args.bucket_name:
        logging.error("An S3 bucket name must be provided. Run \"python flowlog-panda.py -h\"")
        print("An S3 bucket name must be provided. Run \"python flowlog-panda.py -h\"")
        return

    if not args.prefix:
        logging.error("An S3 bucket prefix must be provided. Run \"python flowlog-panda.py -h\"")
        print("An S3 bucket name must be provided. Run \"python flowlog-panda.py -h\"")
        return

    if not args.target_dir:
        logging.error("A Local Directory Name must be provided. Run \"python flowlog-panda.py -h\"")
        print("A Local Directory Name must be provided. Run \"python flowlog-panda.py -h\"")
        return
    
    # Create target dir
    dir = args.target_dir + '/flowlogs'
    pathlib.Path(dir).mkdir(parents=True, exist_ok=True)

    sess    = boto3.Session(profile_name=args.account_profile)
    s3c     = sess.client('s3',region_name='eu-west-2')

    if args.download_data:
        logging.info("Downloading objects from S3")
        print("Downloading objects from S3")
        d = S3helper(args.prefix, dir, args.bucket_name, s3c, args.preserve_keys)
        try:
            dl = d.download_dir()
        except:
             logging.error('Error Downloading objects from S3 %s' % dl)

        #### Get a directory tree
        l = S3helper(args.prefix, dir, args.bucket_name, s3c, args.preserve_keys)
        tree = l.tree()
        print(tree)




if __name__=='__main__':
    main()