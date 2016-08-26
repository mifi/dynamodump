# dynamodump
Node cli for exporting &amp; importing schema of DynamoDB tables. I didn't find any other node tools for dumping table structure (indexes etc), they all only dump data. Can be used in conjunction with other tools to dump data as well.

## Install
```
npm install -g dynamodump
```

## Examples
```
dynamodump export-schema --region=eu-west-1 --table=your-table --file=your-schema-dump
dynamodump import-schema --region=eu-west-1 --file=your-schema-dump --table=your-table --wait-for-active
```

## Related
https://github.com/yegor256/dynamo-archive - Can be used for dumping the data itself
