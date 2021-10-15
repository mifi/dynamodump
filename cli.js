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
const https = require('https');
const process = require('process');

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
      --profile utilize named profile from .aws/credentials file
      --throughput How many rows to delete in parallel (wipe-data)
      --max-retries Set AWS maxRetries
      --ca-file Set SSL certificate authority file
      --marshall Converts JSON to/from DynamoDB record on import/export
      --endpoint Endpoint URL for DynamoDB Local
      --dry-run Report the actions that would be made without actually runnning them.

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

if (cli.flags.maxRetries !== undefined) AWS.config.maxRetries = cli.flags.maxRetries;

const method = methods[cli.input[0]] || cli.showHelp();

if (cli.flags.profile) {
  AWS.config.credentials = new AWS.SharedIniFileCredentials({profile: cli.flags.profile});
}

if (cli.flags.caFile) {
  console.log('Using self signed cert', cli.flags.caFile);
  const ca = fs.readFileSync(cli.flags.caFile);

  AWS.config.update({
    httpOptions: { agent: new https.Agent({ ca }) }
  });
}

bluebird.resolve(method.call(undefined, cli))
  .catch(err => {
    console.error(err.stack);
    process.exit(1);
  });


function listTablesCli(cli) {
  const region = cli.flags.region;
  const endpoint = cli.flags.endpoint;

  return listTables(region, endpoint)
    .then(tables => console.log(tables.join(' ')));
}

function listTables(region, endpoint) {
  const dynamoDb = new AWS.DynamoDB({ region });
  if (endpoint) dynamoDb.endpoint = endpoint;

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
  const dryRun = cli.flags.dryRun;

  if (!tableName) {
    console.error('--table is requred')
    cli.showHelp();
  }

  if(dryRun) {
    console.log('Exporting schema for table', tableName);
    process.exit(0)
  }

  return exportSchema(tableName, cli.flags.file, cli.flags.region, cli.flags.endpoint)
}

function exportAllSchemaCli(cli) {
  const region = cli.flags.region;
  const endpoint = cli.flags.endpoint;
  const dryRun = cli.flags.dryRun;
  return bluebird.map(listTables(region, endpoint), tableName => {
    console.error(`Exporting schema for table ${tableName}`);
    if(!dryRun) {
      return exportSchema(tableName, null, region, endpoint);
    }
  }, { concurrency: 1 });
}

function exportSchema(tableName, file, region, endpoint) {
  const dynamoDb = new AWS.DynamoDB({ region });
  if (endpoint) dynamoDb.endpoint = endpoint;

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
  const endpoint = cli.flags.endpoint;
  const waitForActive = cli.flags.waitForActive;
  const dryRun = cli.flags.dryRun;

  if (!file) {
    console.error('--file is requred')
    cli.showHelp();
  }
  if(dryRun) {
    console.log('Importing schema for table', tableName, 'from file', file);
    process.exit(0)
  }

  const dynamoDb = new AWS.DynamoDB({ region });
  if (endpoint) dynamoDb.endpoint = endpoint;

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
  delete table.TableId;

  if (table.BillingModeSummary) {
    table.BillingMode = table.BillingModeSummary.BillingMode;
  }
  delete table.BillingModeSummary;

  // See https://github.com/mifi/dynamodump/pull/12/files
  if (table.BillingMode === 'PAY_PER_REQUEST') {
    delete table.ProvisionedThroughput;
  }

  function handleIndex(index) {
    // See https://github.com/mifi/dynamodump/pull/12/files
    if (table.BillingMode === 'PAY_PER_REQUEST') {
      delete index.ProvisionedThroughput;
    } else {
      delete index.ProvisionedThroughput.LastIncreaseDateTime;
      delete index.ProvisionedThroughput.LastDecreaseDateTime;
      delete index.ProvisionedThroughput.NumberOfDecreasesToday;
    }
  }

  (table.LocalSecondaryIndexes || []).forEach(index => {
    delete index.IndexSizeBytes;
    delete index.ItemCount;
    delete index.IndexArn;

    handleIndex(index);
  });

  (table.GlobalSecondaryIndexes || []).forEach(index => {
    delete index.IndexStatus;
    delete index.IndexSizeBytes;
    delete index.ItemCount;
    delete index.IndexArn;

    handleIndex(index);
  });
}

function importDataCli(cli) {
  const tableName = cli.flags.table;
  const file = cli.flags.file;
  const region = cli.flags.region;
  const endpoint = cli.flags.endpoint;
  const dryRun = cli.flags.dryRun;

  if (!tableName) {
    console.error('--table is required')
    cli.showHelp();
  }
  if (!file) {
    console.error('--file is required')
    cli.showHelp();
  }
  if(dryRun) {
    console.log('Importing data for table', tableName, 'from file', file);
    process.exit(0)
  }
  let throughput = 1;

  if (cli.flags.throughput !== undefined) {
    if (Number.isInteger(cli.flags.throughput) && cli.flags.throughput > 0) {
      throughput = cli.flags.throughput;
    } else {
      console.error('--throughput must be a positive integer');
      cli.showHelp();
      return;
    }
  }

  const dynamoDb = new AWS.DynamoDB({ region });
  if (endpoint) dynamoDb.endpoint = endpoint;

  const readStream = fs.createReadStream(file);
  const parseStream = JSONStream.parse('*');

  let n = 0;

  const logProgress = () => console.error('Imported', n, 'items');
  const logThrottled = _.throttle(logProgress, 5000, { trailing: false });

  readStream.pipe(parseStream)
    .on('data', data => {
      debug('data');

      if (cli.flags.marshall) {
        data = AWS.DynamoDB.Converter.marshall(data);
      }

      n++;
      logThrottled();

      if (n >= throughput) {
        parseStream.pause();
      }
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
  const dryRun = cli.flags.dryRun;

  if (!tableName) {
    console.error('--table is requred')
    cli.showHelp();
  }
  if(dryRun) {
    console.log('Exporting data for table', tableName);
    process.exit(0)
  }

  return exportData(tableName, cli.flags.file, cli.flags.region, cli.flags.endpoint);
}

function exportAllDataCli(cli) {
  const region = cli.flags.region;
  const endpoint = cli.flags.endpoint;
  const dryRun = cli.flags.dryRun;
  return bluebird.map(listTables(region, endpoint), tableName => {
    console.error(`Exporting data for table ${tableName}`);
    if(!dryRun) {
      return exportData(tableName, null, region, endpoint);
    }
  }, { concurrency: 1 });
}

function exportData(tableName, file, region, endpoint) {
  const dynamoDb = new AWS.DynamoDB({ region });
  if (endpoint) dynamoDb.endpoint = endpoint;

  const file2 = file || sanitizeFilename(tableName + '.dynamodata');
  const writeStream = fs.createWriteStream(file2);
  const stringify = JSONStream.stringify();
  stringify.pipe(writeStream);

  let n = 0;

  const params = { TableName: tableName };
  const scanPage = () => {
    return bluebird.resolve(dynamoDb.scan(params).promise())
      .then(data => {
        data.Items.forEach(item => {
          if (cli.flags.marshall) {
            item = AWS.DynamoDB.Converter.unmarshall(item);
          }
          return stringify.write(item)
        });

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
  const endpoint = cli.flags.endpoint;
  const dryRun = cli.flags.dryRun;
  return bluebird.map(listTables(region, endpoint), tableName => {
    console.error(`Exporting schema and data for table ${tableName}`);
    if(!dryRun) {
      return exportSchema(tableName, null, region, endpoint)
        .then(() => exportData(tableName, null, region, endpoint))
    }
  }, { concurrency: 1 });
}

function wipeDataCli(cli) {
  const tableName = cli.flags.table;
  const dryRun = cli.flags.dryRun;

  if (!tableName) {
    console.error('--table is required')
    cli.showHelp();
  }
  if(dryRun) {
    console.log('Wiping data for table', tableName);
    process.exit(0)
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

  return wipeData(tableName, cli.flags.region, cli.flags.endpoint, throughput);
}

function wipeData(tableName, region, endpoint, throughput) {
  const dynamoDb = new AWS.DynamoDB({ region });
  if 
      (endpoint) dynamoDb.endpoint = endpoint;

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
