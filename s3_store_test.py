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


if __name__ == "__main__": 
    file_name = "/Users/mike/Duke/fintech535/3crypto_pred_mrkt_arb/data/eth-updown-15m-1761708600.json"
    bucket_name = "poly-punting-raw"
    upload_file_to_s3(file_name, bucket_name)