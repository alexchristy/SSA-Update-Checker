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

    def should_upload(self, local_file, s3_path):
        try:
            s3_obj = self.client.head_object(Bucket=self.bucket_name, Key=s3_path)
            s3_file_time = s3_obj['LastModified']
            local_file_time = os.path.getmtime(local_file)
            return local_file_time > s3_file_time.timestamp()
        except Exception as e:
            return True  # if unable to determine, default to uploading

    def sync_folder_to_s3(self, local_directory, s3_prefix, delete_extra_files_in_s3=False):
        # Get a list of all files in the local directory
        local_files = []
        for foldername, subfolders, filenames in os.walk(local_directory):
            for filename in filenames:
                local_files.append(os.path.join(foldername, filename))
        
        # Upload files to S3 if they are new or modified
        for local_file in local_files:
            s3_path = os.path.join(s3_prefix, os.path.relpath(local_file, local_directory))
            if not os.path.exists(local_file):
                continue
            if self.should_upload(local_file, s3_path):
                self.upload_to_s3(local_file, s3_path)
        
        # Delete files from S3 if they aren't in the local directory (optional)
        if delete_extra_files_in_s3:
            s3_files = self.list_s3_files()
            for s3_file in s3_files:
                local_path = os.path.join(local_directory, os.path.relpath(s3_file, s3_prefix))
                if not os.path.exists(local_path):
                    try:
                        self.client.delete_object(Bucket=self.bucket_name, Key=s3_file)
                        logging.info(f"Deleted {s3_file} from S3")
                    except Exception as e:
                        logging.error(f"Error deleting {s3_file} from S3: {e}")
    
    def download_from_s3(self, s3_path, local_path):
        try:
            self.client.download_file(self.bucket_name, s3_path, local_path)
            logging.info(f"Downloaded {s3_path} to {local_path}")
        except Exception as e:
            logging.error(f"Error downloading {s3_path} to {local_path}: {e}")

    def should_download(self, local_file, s3_path):
        try:
            s3_obj = self.client.head_object(Bucket=self.bucket_name, Key=s3_path)
            s3_file_time = s3_obj['LastModified']
            if not os.path.exists(local_file):
                return True
            local_file_time = os.path.getmtime(local_file)
            return s3_file_time.timestamp() > local_file_time
        except Exception as e:
            logging.error(f"Error determining if {s3_path} should be downloaded: {e}")
            return False

    def sync_folder_from_s3(self, local_directory, s3_prefix, delete_extra_files_locally=False):
        # List all files under the specified s3_prefix
        s3_files = self.list_s3_files()
        relevant_s3_files = [f for f in s3_files if f.startswith(s3_prefix)]
        
        # Download new or modified files from S3 to the local directory
        for s3_file in relevant_s3_files:
            local_path = os.path.join(local_directory, os.path.relpath(s3_file, s3_prefix))
            local_dir = os.path.dirname(local_path)
            try:
                if not os.path.exists(local_dir):
                    os.makedirs(local_dir)
                if self.should_download(local_path, s3_file):
                    self.download_from_s3(s3_file, local_path)
            except PermissionError:
                logging.error(f"Permission denied when trying to create directory or write to {local_path}.")
            except OSError as e:
                if e.errno == 28:  # errno 28 is "No space left on device"
                    logging.error(f"No space left on device when trying to download to {local_path}.")
                else:
                    logging.error(f"OS error encountered: {e}.")

        # Delete files from local directory that don't exist in the S3 bucket (optional)
        if delete_extra_files_locally:
            local_files = set()
            for foldername, subfolders, filenames in os.walk(local_directory):
                for filename in filenames:
                    rel_path = os.path.relpath(os.path.join(foldername, filename), local_directory)
                    local_files.add(os.path.join(s3_prefix, rel_path))
            
            for local_file in local_files:
                if local_file not in s3_files:
                    try:
                        os.remove(os.path.join(local_directory, os.path.relpath(local_file, s3_prefix)))
                        logging.info(f"Deleted {local_file} from local directory")
                    except PermissionError:
                        logging.error(f"Permission denied when trying to delete {local_file}.")
                    except Exception as e:
                        logging.error(f"Error deleting {local_file} from local directory: {e}.")
