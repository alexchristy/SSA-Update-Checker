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
        
    def move_object(self, source_key, destination_key):
        try:
            # Copy the object to the new location
            copy_source = {'Bucket': self.bucket_name, 'Key': source_key}
            self.client.copy_object(CopySource=copy_source, Bucket=self.bucket_name, Key=destination_key)
            logging.info(f"Successfully copied object from {source_key} to {destination_key} in bucket {self.bucket_name}.")

            # Delete the original object
            self.client.delete_object(Bucket=self.bucket_name, Key=source_key)
            logging.info(f"Successfully deleted object from {source_key} in bucket {self.bucket_name}.")

        except Exception as e:
            logging.error(f"Failed to move object from {source_key} to {destination_key}. Error: {str(e)}")
            raise

    def create_directory(self, path):
        try:
            # Make sure the path ends with a '/'
            if not path.endswith('/'):
                path += '/'

            # Creating an empty object representing the directory
            self.client.put_object(Bucket=self.bucket_name, Key=path, Body='')
            logging.info(f"Successfully created directory at {path} in bucket {self.bucket_name}.")

        except Exception as e:
            logging.error(f"Failed to create directory at {path}. Error: {str(e)}")
            raise
    
    def directory_exists(self, path):
        try:
            # Make sure the path ends with a '/'
            if not path.endswith('/'):
                path += '/'

            # List objects with the given prefix (i.e., directory)
            response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=path, MaxKeys=1)
            
            # Check if any objects are returned with the given prefix
            if 'Contents' in response and response['Contents']:
                logging.info(f"Directory {path} exists in bucket {self.bucket_name}.")
                return True
            else:
                logging.info(f"Directory {path} does not exist in bucket {self.bucket_name}.")
                return False

        except Exception as e:
            logging.error(f"Failed to check if directory {path} exists in bucket {self.bucket_name}. Error: {str(e)}")
            raise