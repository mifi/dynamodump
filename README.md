# dynamodump 🕋→💾
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
dynamodump wipe-data --throughput 5 --table your-table --region eu-west-1
AWS_PROFILE=customprofile dynamodump list-tables
```

### DynamoDB Local

See [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html).
Use the flag `--endpoint`, e.g. `--endpoint http://localhost:8000`

## Backup all tables with structure/data to s3
```
(mkdir backup && cd backup && dynamodump export-all --region eu-west-1) &&
aws s3 cp backup s3://mybucket/ --recursive
```

## Notes
- Stream ARN will not be preserved during export/import

## Alternatives
If you're doing a simple restore/backup job you may consider using AWS native [Backup Restore](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BackupRestore.html) feature (or Point In Time Restore), priced at 0.1x c per GB it can be significantly cheaper for larger tables (100k+ rows), easier to avoid issues with autoscaling/request throttling and shifts the extra load away from your tables.

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
- https://github.com/mifi/cognito-backup
