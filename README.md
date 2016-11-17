# dynamodump
Node cli for exporting &amp; importing schema and data from DynamoDB tables. I didn't find any other node tools for dumping table schema (structure, indexes etc), they all just dump data.

## Install
```
npm install -g dynamodump
```

## Usage
Run:
```
dynamodump
```

## Examples
```
dynamodump export-schema --region=eu-west-1 --table=your-table --file=your-schema-dump
dynamodump import-schema --region=eu-west-1 --file=your-schema-dump --table=your-table --wait-for-active
dynamodump export-all-data --region=eu-west-1
dynamodump import-data --region=eu-west-1 --table=mikael-test --file=mikael-test.dynamodata
```

## Backup all tables with structure/data to s3
```
(mkdir backup && cd backup && dynamodump export-all --region eu-west-1) &&
aws s3 cp backup s3://mybucket/ --recursive
```

## Notes
- Stream ARN will not be preserved during export/import

## TODO
- Batch import: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#batchWriteItem-property

## Related
- https://github.com/triploc/dynq
- https://github.com/Purple-Unicorns/DynamoDbBackUp
- https://github.com/markitx/dynamo-backup-to-s3
- https://github.com/mapbox/dynamodb-migrator
- https://github.com/PageUpPeopleOrg/dynamodb-replicator
- https://github.com/bchew/dynamodump
- https://github.com/yegor256/dynamo-archive
