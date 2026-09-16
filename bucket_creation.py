import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test", 
    region_name="us-east-1"
)
s3.create_bucket(Bucket="nyc311-bucket")

#### Validate the bucket creation
response = s3.list_buckets()
print("Existing buckets:")
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')