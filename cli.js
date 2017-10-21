#!/usr/bin/env node
'use strict';

const meow = require('meow');
const AWS = require('aws-sdk');
const bluebird = require('bluebird');
const fs = require('fs');
const sanitizeFilename = require('sanitize-filename');
const promisePoller = require('promise-poller').default;
const JSONStream = require('JSONStream');
const streamToPromise = require('stream-to-promise');
const debug = require('debug')('dynamodump');
const _ = require('lodash');

bluebird.promisifyAll(fs);

const cli = meow(`
    Usage
      $ dynamodump list-tables <options>  List tables, separated by space
      $ dynamodump export-schema <options>  Export schema of a table
      $ dynamodump import-schema <options>  Import schema of a table (creates the table)
      $ dynamodump export-all-schema <options>  Export schema of all tables
      $ dynamodump export-data <options>  Export all data of a table
      $ dynamodump import-data <options>  Import all data into a table
      $ dynamodump export-all-data <options>  Export data from all tables
      $ dynamodump export-all <options>  Export data and schema from all tables
      $ dynamodump wipe-data <options>  Wipe all data from a table

      AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
      is specified in env variables or ~/.aws/credentials

    Options
      --region AWS region
      --file File name to export to or import from (defaults to table_name.dynamoschema and table_name.dynamodata)
      --table Table to export. When importing, this will override the TableName from the schema dump file
      --wait-for-active Wait for table to become active when importing schema
      --throughput How many rows to delete in parallel (wipe-data)

    Examples
      dynamodump export-schema --region=eu-west-1 --table=your-table --file=your-schema-dump
      dynamodump import-schema --region=eu-west-1 --file=your-schema-dump --table=your-table --wait-for-active
      dynamodump export-all-data --region=eu-west-1
      dynamodump import-data --region=eu-west-1 --table=mikael-test --file=mikael-test.dynamodata
      dynamodump wipe-data --region=eu-west-1 --table=mikael-test --throughput=10
      `);

const methods = {
  'export-schema': exportSchemaCli,
  'import-schema': importSchemaCli,
  'list-tables': listTablesCli,
  'export-all-schema': exportAllSchemaCli,
  'export-data': exportDataCli,
  'export-all-data': exportAllDataCli,
  'export-all': exportAllCli,
  'import-data': importDataCli,
  'wipe-data': wipeDataCli
};

const method = methods[cli.input[0]] || cli.showHelp();

bluebird.resolve(method.call(undefined, cli))
  .catch(err => {
    console.error(err.stack);
    process.exit(1);
  });


function listTablesCli(cli) {
  const region = cli.flags.region;

  return listTables(region)
    .then(tables => console.log(tables.join(' ')));
}

function listTables(region) {
  const dynamoDb = new AWS.DynamoDB({ region });

  const params = {};

  let tables = [];
  const listTablesPaged = () => {
    return dynamoDb.listTables(params).promise()
      .then(data => {
        tables = tables.concat(data.TableNames);
        if (data.LastEvaluatedTableName !== undefined) {
          params.ExclusiveStartTableName = data.LastEvaluatedTableName;
          return listTablesPaged();
        }

        return tables;
      });
  };

  return listTablesPaged();
}

function exportSchemaCli(cli) {
  const tableName = cli.flags.table;

  if (!tableName) {
    console.error('--table is requred')
    cli.showHelp();
  }

  return exportSchema(tableName, cli.flags.file, cli.flags.region)
}

function exportAllSchemaCli(cli) {
  const region = cli.flags.region;
  return bluebird.map(listTables(region), tableName => {
    console.error(`Exporting ${tableName}`);
    return exportSchema(tableName, null, region);
  }, { concurrency: 1 });
}

function exportSchema(tableName, file, region) {
  const dynamoDb = new AWS.DynamoDB({ region });

  return dynamoDb.describeTable({ TableName: tableName }).promise()
    .then(data => {
      const table = data.Table;
      const file2 = file || sanitizeFilename(tableName + '.dynamoschema');

      return fs.writeFileAsync(file2, JSON.stringify(table, null, 2))
    });
}

function importSchemaCli(cli) {
  const tableName = cli.flags.table;
  const file = cli.flags.file;
  const region = cli.flags.region;
  const waitForActive = cli.flags.waitForActive;

  if (!file) {
    console.error('--file is requred')
    cli.showHelp();
  }

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

      filterTable(json);

      return dynamoDb.createTable(json).promise()
        .then(() => {
          if (waitForActive !== undefined) {
            return doWaitForActive();
          }
        });
    });
}

function filterTable(table) {
  delete table.TableStatus;
  delete table.CreationDateTime;
  delete table.ProvisionedThroughput.LastIncreaseDateTime;
  delete table.ProvisionedThroughput.LastDecreaseDateTime;
  delete table.ProvisionedThroughput.NumberOfDecreasesToday;
  delete table.TableSizeBytes;
  delete table.ItemCount;
  delete table.TableArn;
  delete table.LatestStreamLabel;
  delete table.LatestStreamArn;

  (table.LocalSecondaryIndexes || []).forEach(index => {
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
}

function importDataCli(cli) {
  const tableName = cli.flags.table;
  const file = cli.flags.file;
  const region = cli.flags.region;

  if (!tableName) {
    console.error('--table is requred')
    cli.showHelp();
  }
  if (!file) {
    console.error('--file is requred')
    cli.showHelp();
  }

  const dynamoDb = new AWS.DynamoDB({ region });

  const readStream = fs.createReadStream(file);
  const parseStream = JSONStream.parse('*');

  let n = 0;

  const logProgress = () => console.error('Imported', n, 'items');
  const logThrottled = _.throttle(logProgress, 5000, { trailing: false });

  readStream.pipe(parseStream)
    .on('data', data => {
      debug('data');

      n++;
      logThrottled();

      parseStream.pause();
      dynamoDb.putItem({ TableName: tableName, Item: data }).promise()
        .then(() => parseStream.resume())
        .catch(err => parseStream.emit('error', err));
    });

  return new Promise((resolve, reject) => {
    parseStream.on('end', resolve);
    parseStream.on('error', reject);
  })
    .then(() => logProgress());
}

function exportDataCli(cli) {
  const tableName = cli.flags.table;

  if (!tableName) {
    console.error('--table is requred')
    cli.showHelp();
  }

  return exportData(tableName, cli.flags.file, cli.flags.region);
}

function exportAllDataCli(cli) {
  const region = cli.flags.region;
  return bluebird.map(listTables(region), tableName => {
    console.error(`Exporting ${tableName}`);
    return exportData(tableName, null, region);
  }, { concurrency: 1 });
}

function exportData(tableName, file, region) {
  const dynamoDb = new AWS.DynamoDB({ region });

  const file2 = file || sanitizeFilename(tableName + '.dynamodata');
  const writeStream = fs.createWriteStream(file2);
  const stringify = JSONStream.stringify();
  stringify.pipe(writeStream);

  let n = 0;

  const params = { TableName: tableName };
  const scanPage = () => {
    return bluebird.resolve(dynamoDb.scan(params).promise())
      .then(data => {
        data.Items.forEach(item => stringify.write(item));

        n += data.Items.length;
        console.error('Exported', n, 'items');

        if (data.LastEvaluatedKey !== undefined) {
          params.ExclusiveStartKey = data.LastEvaluatedKey;
          return scanPage();
        }
      });
  }

  return scanPage()
    .finally(() => {
      stringify.end();
      return streamToPromise(stringify);
    })
    .finally(() => writeStream.end());
}

function exportAllCli(cli) {
  const region = cli.flags.region;
  return bluebird.map(listTables(region), tableName => {
    console.error(`Exporting ${tableName}`);
    return exportSchema(tableName, null, region)
      .then(() => exportData(tableName, null, region))
  }, { concurrency: 1 });
}

function wipeDataCli(cli) {
  const tableName = cli.flags.table;

  if (!tableName) {
    console.error('--table is requred')
    cli.showHelp();
  }

  let throughput = 10;

  if (cli.flags.throughput !== undefined) {
    if (Number.isInteger(cli.flags.throughput) && cli.flags.throughput > 0) {
      throughput = cli.flags.throughput;
    } else {
      console.error('--throughput must be a positive integer');
      cli.showHelp();
    }
  }

  return wipeData(tableName, cli.flags.region, throughput);
}

function wipeData(tableName, region, throughput) {
  const dynamoDb = new AWS.DynamoDB({ region });

  let n = 0;

  const params = {
    TableName: tableName,
    Limit: throughput
  };

  const scanPage = (keyFields) => {
    return bluebird.resolve(dynamoDb.scan(params).promise())
      .then(data => {
        return bluebird.map(data.Items, item => {
          const delParams = {
            TableName: tableName,
            Key: _.pick(item, keyFields)
          };
          return dynamoDb.deleteItem(delParams).promise();
        }).then(() => {
          n += data.Items.length;
          console.error('Wiped', n, 'items');

          if (data.LastEvaluatedKey !== undefined) {
            params.ExclusiveStartKey = data.LastEvaluatedKey;
            return scanPage(keyFields);
          }
        });
      });
  }

  return dynamoDb.describeTable({ TableName: tableName }).promise()
    .then((table) => {
      const hashKeyElement = _.filter(table.Table.KeySchema, entry => entry.KeyType === 'HASH');
      const rangeKeyElement = _.filter(table.Table.KeySchema, entry => entry.KeyType === 'RANGE');

      const keyFields = [];
      keyFields.push(hashKeyElement[0].AttributeName);
      if (rangeKeyElement && rangeKeyElement.length > 0) {
        keyFields.push(rangeKeyElement[0].AttributeName);
      }

      return scanPage(keyFields);
    });
}
