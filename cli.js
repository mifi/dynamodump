#!/usr/bin/env node
'use strict';

const meow = require('meow');
const AWS = require('aws-sdk');
const bluebird = require('bluebird');
const fs = require('fs');
const sanitizeFilename = require('sanitize-filename');
const promisePoller = require('promise-poller').default;

bluebird.promisifyAll(fs);

const cli = meow(`
    Usage
      $ dynamodump export-schema <options>
      $ dynamodump import-schema <options>

      AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
      is specified in env variables or ~/.aws/credentials

    Options
      --region AWS region
      --file File name to export to (defaults to sanitized table name), or import from
      --table Table to export. When importing this will override the TableName from the dump file
      --wait-for-active Wait for table to become active when importing

    Examples
      dynamodump export-schema --region=eu-west-1 --table=your-table --file=your-schema-dump
      dynamodump import-schema --region=eu-west-1 --file=your-schema-dump --table=your-table --wait-for-active
`);

const methods = {
  'export-schema': exportSchema,
  'import-schema': importSchema,
};

const method = methods[cli.input[0]] || cli.showHelp();

bluebird.resolve(method.call(undefined, cli))
  .catch(err => console.error(err));


function exportSchema(cli) {
  const tableName = cli.flags.table || cli.showHelp();
  const dynamoDb = new AWS.DynamoDB({ region: cli.flags.region });

  return dynamoDb.describeTable({ TableName: tableName }).promise()
    .then(data => {
      const table = data.Table;

      delete table.TableStatus;
      delete table.CreationDateTime;
      delete table.ProvisionedThroughput.LastIncreaseDateTime;
      delete table.ProvisionedThroughput.NumberOfDecreasesToday;
      delete table.TableSizeBytes;
      delete table.ItemCount;
      delete table.TableArn;

      (table.LocalSecondaryIndexes || []).forEach(index => {
        delete index.IndexSizeBytes;
        delete index.ItemCount;
        delete index.IndexArn;
      });

      (table.GlobalSecondaryIndexes || []).forEach(index => {
        delete index.IndexStatus;
        delete index.IndexSizeBytes;
        delete index.ItemCount;
        delete index.IndexArn;
      });

      const file = cli.flags.file || sanitizeFilename(tableName);

      return fs.writeFileAsync(file, JSON.stringify(table, null, 2))
    });
}

function importSchema(cli) {
  const tableName = cli.flags.table;
  const file = cli.flags.file || cli.showHelp();
  const region = cli.flags.region;
  const waitForActive = cli.flags.waitForActive;

  const dynamoDb = new AWS.DynamoDB({ region });

  const doWaitForActive = () => promisePoller({
    taskFn: () => {
      return dynamoDb.describeTable({ TableName: tableName }).promise()
        .then(data => {
          if (data.Table.TableStatus !== 'ACTIVE') throw new Error();
        });
    },
    interval: 1000,
    retries: 60
  });

  fs.readFileAsync(file)
    .then(data => JSON.parse(data))
    .then(json => {
      if (tableName) json.TableName = tableName;
      return dynamoDb.createTable(json).promise()
        .then(() => {
          if (waitForActive !== undefined) {
            return doWaitForActive();
          }
        });
    });
}
