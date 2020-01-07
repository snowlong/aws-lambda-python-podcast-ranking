import json
import boto3
import re
import requests
from datetime import datetime

s3 = boto3.resource('s3')
s3client = boto3.client('s3')

root_bucket = 'apple-ranking'
root_prefix = '/current'
dist_bucket = 'apple-ranking'
dist_prefix = '/archive'

def copy_and_delete(source='', root_prefix='', dist_bucket='', dist_prefix='', dryrun=False):
  contents_count = 0
  marker = None

  while True:
    if marker:
        response = s3client.list_objects(
          Bucket=root_bucket,
          Prefix=root_prefix,
          Marker=marker
        )
    else:
        response = s3client.list_objects(
          Bucket=root_bucket,
          Prefix=root_prefix
        )

    if 'Contents' in response:
        contents = response['Contents']
        contents_count = contents_count + len(contents)
        for content in contents:
            relative_prefix = re.sub('^' + root_prefix, '', content['Key'])
            if not dryrun:
                print('Copying: s3://' + root_bucket + '/' + content['Key'] + ' To s3://' + dist_bucket + '/' + dist_prefix + relative_prefix)
                s3client.copy_object(
                  Bucket=dist_bucket,
                  Key=dist_prefix + relative_prefix,
                  CopySource={'Bucket': root_bucket, 'Key': content['Key']}
                )
                s3client.delete_object(
                  Bucket=root_bucket,
                  Key=content['Key']
                )
            else:
                print('DryRun: s3://' + root_bucket + '/' + content['Key'] + ' To s3://' + dist_bucket + '/' + dist_prefix + relative_prefix)

    if response['IsTruncated']:
        marker = response['Contents'][-1]['Key']
    else:
        break

  print(contents_count)

def lambda_handler(event, context):
  copy_and_delete(root_bucket, root_prefix, dist_bucket, dist_prefix, False)

  key = root_prefix.replace('/', '') + '/ranking.json'
  key_time = root_prefix.replace('/', '') + '/ranking_' + datetime.now().strftime('%Y%m%d%H%M%S') + '.json'

  headers = {"content-type": "application/json"}
  res = requests.get(
    'https://itunes.apple.com/jp/rss/toppodcasts/genre=1318/explicit=true/limit=100/json',
    headers=headers
  )

  # obj = s3.Object(root_bucket, key)
  # obj.put( Body = bytearray(json.dumps(res.json()), 'UTF-8'))

  s3client.put_object(
    Key=key,
    Bucket=root_bucket,
    Body=bytearray(json.dumps(res.json()), 'UTF-8'),
  )

  # obj_t = s3.Object(root_bucket, key_time)
  # obj_t.put( Body = bytearray(json.dumps(res.json()), 'UTF-8'))

  s3client.put_object(
    Key=key_time,
    Bucket=root_bucket,
    Body=bytearray(json.dumps(res.json()), 'UTF-8'),
  )

  return
