import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)
#s3.create_bucket(Bucket="test-bucket")
#print(s3.list_buckets())

try:
    s3.head_object(Bucket="nyc311-bucket", Key="bronze/definitely-not-real.json")
except Exception as e:
    #print(type(e))
    #print(e)
    print(e.response)