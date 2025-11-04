import boto3 


s3 = boto3.client('s3') 

def upload_file_to_s3(file_name, bucket, object_name=None): 
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    if object_name is None:
        object_name = file_name.split('/')[-1]

    try:
        s3.upload_file(file_name, bucket, object_name)
    except Exception as e:
        print(f"Error uploading file {file_name} to bucket {bucket}: {e}")
        return False
    return True


def download_file_from_s3(bucket, object_name, file_name=None):
    """Download a file from an S3 bucket

    :param bucket: Bucket to download from
    :param object_name: S3 object name
    :param file_name: File to download to. If not specified then object_name is used
    :return: True if file was downloaded, else False
    """

    if file_name is None:
        file_name = object_name

    try:
        s3.download_file(bucket, object_name, file_name)
    except Exception as e:
        print(f"Error downloading file {object_name} from bucket {bucket}: {e}")
        return False
    return True


def get_s3_bucket_contents(bucket):
    """List contents of an S3 bucket

    :param bucket: Bucket to list contents of
    :return: List of object names in the bucket
    """
    try:
        response = s3.list_objects_v2(Bucket=bucket)
        if "Contents" in response:
            return [item["Key"] for item in response["Contents"]]
        else:
            return []
    except Exception as e:
        print(f"Error listing contents of bucket {bucket}: {e}")
        return []


if __name__ == "__main__":
    bucket_name = "poly-punting-raw"
    # file_name = "/Users/mike/Duke/fintech535/3crypto_pred_mrkt_arb/data/eth-updown-15m-1761708600.json"

    # upload_file_to_s3(file_name, bucket_name)

    contents = get_s3_bucket_contents(bucket_name)
    print("Bucket Contents:")
    for item in contents:
        print(f" - {item}")
