from minio import Minio
import io

class MinioClient:
    def __init__(self, host, access_key, secret_key, secure=False):
        self.client = Minio(
            host,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )

    def create_bucket(self, bucket_name):
        """Create a new bucket if it doesn't already exist."""
        if not self.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")
        else:
            print(f"Bucket {bucket_name} already exists.")

    def bucket_exists(self, bucket_name):
        """Check if a bucket exists."""
        try:
            # Attempt to get the bucket's metadata
            self.client.stat_bucket(bucket_name)
            return True
        except Exception as e:
            # If the bucket doesn't exist, an error is raised
            return False

    def list_objects(self, bucket_name):
        """List objects in a bucket."""
        return self.client.list_objects(bucket_name)

    def download_object(self, bucket_name, object_name, file_path):
        """Download an object from a MinIO bucket."""
        self.client.fget_object(bucket_name, object_name, file_path)
        print(f"Downloaded {object_name} to {file_path}")

    def upload_object(self, bucket_name, object_name, data, content_type):
        """Upload an object to a MinIO bucket."""
        self.client.put_object(
            bucket_name, object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type=content_type
        )
        print(f"Uploaded {object_name} to {bucket_name}")
