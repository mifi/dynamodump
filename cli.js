#!/usr/bin/env node
'use strict';

import meow from 'meow';
import AWS from 'aws-sdk';
import pMap from 'p-map';
import { readFile, writeFile } from 'fs/promises';
import { createReadStream, createWriteStream } from 'fs';
import sanitizeFilename from 'sanitize-filename';
import JSONStream from 'JSONStream';
import { pipeline as pipelineCb } from 'stream';
import { promisify } from 'util';
import Debug from 'debug';
import throttle from 'lodash/throttle.js';
import pick from 'lodash/pick.js';
import https from 'https';

const pipeline = promisify(pipelineCb);


const debug = Debug('dynamodump');

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
    --marshall Converts JSON to/from DynamoDB record on import/export
    --endpoint Endpoint URL for DynamoDB Local
    --ca-file Set SSL certificate authority file
    --stack-trace Log stack trace upon error

  Examples
    dynamodump export-schema --region=eu-west-1 --table=your-table --file=your-schema-dump
    dynamodump import-schema --region=eu-west-1 --file=your-schema-dump --table=your-table --wait-for-active
    dynamodump export-all-data --region=eu-west-1
    dynamodump import-data --region=eu-west-1 --table=mikael-test --file=mikael-test.dynamodata
    dynamodump wipe-data --region=eu-west-1 --table=mikael-test --throughput=10
    `,
{
  importMeta: import.meta,
  flags: {
    stackTrace: {
      type: 'boolean',
    }
  }
});

if (cli.flags.maxRetries !== undefined) AWS.config.maxRetries = cli.flags.maxRetries;

if (cli.flags.profile) {
  AWS.config.credentials = new AWS.SharedIniFileCredentials({profile: cli.flags.profile});
}

if (cli.flags.caFile) {
  console.log('Using self signed cert', cli.flags.caFile);
  const ca = await readFile(cli.flags.caFile);

  AWS.config.update({
    httpOptions: { agent: new https.Agent({ ca }) }
  });
}

async function listTablesCli(cli) {
  const region = cli.flags.region;
  const endpoint = cli.flags.endpoint;

  const tables = await listTables(region, endpoint);
  console.log(tables.join(' '));
}

function listTables(region, endpoint) {
  const dynamoDb = new AWS.DynamoDB({ region });
  if (endpoint) dynamoDb.endpoint = endpoint;

  const params = {};

  let tables = [];
  async function listTablesPaged() {
    const data = await dynamoDb.listTables(params).promise();
    tables = tables.concat(data.TableNames);
    if (data.LastEvaluatedTableName !== undefined) {
      params.ExclusiveStartTableName = data.LastEvaluatedTableName;
      return listTablesPaged();
    }

    return tables;
  };

  return listTablesPaged();
}

async function exportSchemaCli(cli) {
  const tableName = cli.flags.table;

  if (!tableName) {
    console.error('--table is requred')
    cli.showHelp();
  }

  return exportSchema(tableName, cli.flags.file, cli.flags.region, cli.flags.endpoint)
}

async function exportAllSchemaCli(cli) {
  const region = cli.flags.region;
  const endpoint = cli.flags.endpoint;

  return pMap(await listTables(region, endpoint), tableName => {
    console.error(`Exporting ${tableName}`);
    return exportSchema(tableName, null, region, endpoint);
  }, { concurrency: 1 });
}

async function exportSchema(tableName, file, region, endpoint) {
  const dynamoDb = new AWS.DynamoDB({ region });
  if (endpoint) dynamoDb.endpoint = endpoint;

  const data = await dynamoDb.describeTable({ TableName: tableName }).promise();
  const table = data.Table;
  const file2 = file || sanitizeFilename(tableName + '.dynamoschema');

  return writeFile(file2, JSON.stringify(table, null, 2))
}

async function importSchemaCli(cli) {
  const tableName = cli.flags.table;
  const file = cli.flags.file;
  const region = cli.flags.region;
  const endpoint = cli.flags.endpoint;
  const waitForActive = cli.flags.waitForActive;

  if (!file) {
    console.error('--file is requred')
    cli.showHelp();
  }

  const dynamoDb = new AWS.DynamoDB({ region });
  if (endpoint) dynamoDb.endpoint = endpoint;

  async function doWaitForActive() {
    const retries = 60;
    for (let i = 0; i < retries; i += 1) {
      const data = await dynamoDb.describeTable({ TableName: tableName }).promise();
      if (data.Table.TableStatus === 'ACTIVE') return;
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    throw new Error('Timed out');
  }

  const json = await readFile(file)
  const data = JSON.parse(json)
  if (tableName) data.TableName = tableName;

  filterTable(data);

  await dynamoDb.createTable(data).promise()
  if (waitForActive !== undefined) return doWaitForActive();
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

async function importDataCli(cli) {
  const tableName = cli.flags.table;
  const file = cli.flags.file;
  const region = cli.flags.region;
  const endpoint = cli.flags.endpoint;

  if (!tableName) {
    console.error('--table is required')
    cli.showHelp();
  }
  if (!file) {
    console.error('--file is required')
    cli.showHelp();
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

  const readStream = createReadStream(file);
  const parseStream = JSONStream.parse('*');

  let n = 0;

  const logProgress = () => console.error('Imported', n, 'items');
  const logThrottled = throttle(logProgress, 5000, { trailing: false });

  readStream.pipe(parseStream)
    .on('data', async (data) => {
      debug('data');

      if (cli.flags.marshall) {
        data = AWS.DynamoDB.Converter.marshall(data);
      }

      n++;

      if (n >= throughput) {
        parseStream.pause();
      }

      try {
        await dynamoDb.putItem({ TableName: tableName, Item: data }).promise();
        logThrottled();
        parseStream.resume();
      } catch (err) {
        parseStream.emit('error', err);
      }
    });

  await new Promise((resolve, reject) => {
    parseStream.on('end', resolve);
    parseStream.on('error', reject);
  })
}

async function exportDataCli(cli) {
  const tableName = cli.flags.table;

  if (!tableName) {
    console.error('--table is requred')
    cli.showHelp();
  }

  return exportData(tableName, cli.flags.file, cli.flags.region, cli.flags.endpoint);
}

async function exportAllDataCli(cli) {
  const region = cli.flags.region;
  const endpoint = cli.flags.endpoint;

  return pMap(await listTables(region, endpoint), tableName => {
    console.error(`Exporting ${tableName}`);
    return exportData(tableName, null, region, endpoint);
  }, { concurrency: 1 });
}

async function exportData(tableName, file, region, endpoint) {
  const dynamoDb = new AWS.DynamoDB({ region });
  if (endpoint) dynamoDb.endpoint = endpoint;

  const file2 = file || sanitizeFilename(tableName + '.dynamodata');
  const writeStream = createWriteStream(file2);
  const stringify = JSONStream.stringify();

  let n = 0;

  const params = { TableName: tableName };

  async function scanPage() {
    const data = await dynamoDb.scan(params).promise();

    data.Items.forEach((item) => {
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
    } else {
      stringify.end();
    }
  }

  scanPage()
  await pipeline(stringify, writeStream);
}

async function exportAllCli(cli) {
  const region = cli.flags.region;
  const endpoint = cli.flags.endpoint;

  return pMap(await listTables(region, endpoint), async (tableName) => {
    console.error(`Exporting ${tableName}`);
    await exportSchema(tableName, null, region, endpoint);
    await exportData(tableName, null, region, endpoint);
  }, { concurrency: 1 });
}

async function wipeDataCli(cli) {
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

  return wipeData(tableName, cli.flags.region, cli.flags.endpoint, throughput);
}

async function wipeData(tableName, region, endpoint, throughput) {
  const dynamoDbParams = { region };
  if (endpoint) dynamoDbParams.endpoint = endpoint;
  const dynamoDb = new AWS.DynamoDB(dynamoDbParams);

  let n = 0;

  const params = {
    TableName: tableName,
    Limit: throughput
  };

  async function scanPage(keyFields) {
    const data = await dynamoDb.scan(params).promise()

    await pMap(data.Items, (item) => {
      const delParams = {
        TableName: tableName,
        Key: pick(item, keyFields)
      };
      return dynamoDb.deleteItem(delParams).promise();
    }, { concurrency: 10 })

    n += data.Items.length;
    console.error('Wiped', n, 'items');

    if (data.LastEvaluatedKey !== undefined) {
      params.ExclusiveStartKey = data.LastEvaluatedKey;
      return scanPage(keyFields);
    }
  }

  const table = await dynamoDb.describeTable({ TableName: tableName }).promise();
  const hashKeyElement = table.Table.KeySchema.filter((entry) => entry.KeyType === 'HASH');
  const rangeKeyElement = table.Table.KeySchema.filter((entry) => entry.KeyType === 'RANGE');

  const keyFields = [];
  keyFields.push(hashKeyElement[0].AttributeName);
  if (rangeKeyElement && rangeKeyElement.length > 0) {
    keyFields.push(rangeKeyElement[0].AttributeName);
  }

  return scanPage(keyFields);
}

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

try {
  await method(cli)
} catch (err) {
  if (cli.flags.stackTrace) {
    console.error('Error:', err);
  } else {
    console.error('Error:', err.message);
  }
  process.exitCode = 1;
}
