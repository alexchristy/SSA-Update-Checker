import boto3
import os
import logging


class s3Bucket:

    def __init__(self):
        # Get keys from enviroment variables
        env_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        env_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        env_bucket_name = os.environ.get('AWS_BUCKET_NAME')

        # Check enviroment variables
        if not env_access_key_id or not env_secret_access_key or not env_bucket_name:
            logging.error("AWS configuration missing in environment variables!")
            raise EnvironmentError("Missing AWS configuration in environment variables.")

        # Initialize bucket and client
        self.bucket_name = env_bucket_name
        self.client = boto3.client('s3', aws_access_key_id=env_access_key_id, aws_secret_access_key=env_secret_access_key)

    def upload_to_s3(self, local_path, s3_path):
        try:
            self.client.upload_file(local_path, self.bucket_name, s3_path)
            logging.info(f"Uploaded {local_path} to {s3_path}")
        except Exception as e:
            logging.error(f"Error uploading {local_path} to {s3_path}: {e}")

    def list_s3_files(self, s3_prefix=''):
        try:
            response = self.client.list_objects(Bucket=self.bucket_name, Prefix=s3_prefix)
            
            # Check if 'Contents' key exists in the response
            if 'Contents' in response:
                return [item['Key'] for item in response['Contents']]
            else:
                logging.warning(f"No files found in S3 with prefix: {s3_prefix}")
                return []
                
        except Exception as e:
            logging.error(f"Error listing files in S3: {e}")
            return []
    
    def download_from_s3(self, s3_path, local_path):
        try:
            self.client.download_file(self.bucket_name, s3_path, local_path)
            logging.info(f"Downloaded {s3_path} to {local_path}")
        except Exception as e:
            logging.error(f"Error downloading {s3_path} to {local_path}: {e}")