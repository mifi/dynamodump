#!/usr/bin/env node
'use strict';

import meow from 'meow';
import pMap from 'p-map';
import { readFile, writeFile } from 'fs/promises';
import { createReadStream, createWriteStream, readFileSync } from 'fs';
import sanitizeFilename from 'sanitize-filename';
import JSONStream from 'JSONStream';
import { pipeline as pipelineCb } from 'stream';
import { promisify } from 'util';
import Debug from 'debug';
import throttle from 'lodash/throttle.js';
import pick from 'lodash/pick.js';
import {Agent} from 'https';
import {
  DynamoDBClient,
  DescribeTableCommand,
  ListTablesCommand,
  ScanCommand,
  DeleteItemCommand,
  PutItemCommand,
  CreateTableCommand
} from '@aws-sdk/client-dynamodb';
import { fromIni } from '@aws-sdk/credential-providers'
import { NodeHttpHandler } from '@aws-sdk/node-http-handler';
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb"

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
    --max-attempts Set AWS maxAttempts
    --marshall Converts JSON to/from DynamoDB record on import/export
    --endpoint Endpoint URL for DynamoDB Local
    --ca-file Set SSL certificate authority file
    --stack-trace Log stack trace upon error
    --dry-run Report the actions that would be made without actually runnning them.
    --log-level Set log level: debug, info, warn, error

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
        },
        waitForActive: {
          type: 'boolean',
        },
        dryRun: {
          type: 'boolean',
        },
      }
    });

const logger = (() => {
  const levels = [
    'debug',
    'info',
    'warn',
    'error',
  ];
  const currentLogLevelIndex = levels.indexOf(cli.flags.logLevel || 'info');

  const log = (level, ...args) => {
    if (levels.indexOf(level) < currentLogLevelIndex) return;
    if (cli.flags.quiet) return;
    if (level === 'error') return console.error(...args);
    if (level === 'warn') return console.warn(...args);
    console.log(...args);
  }
  return {
    error: (...args) => log('error', ...args),
    info: (...args) => log('info', ...args),
    warn: (...args) => log('warn', ...args),
    debug: (...args) => log('debug', ...args),
  }
})();



const { dryRun, table: tableName  } = cli.flags;

function createDynamoDb() {
  const { region, endpoint, maxAttempts, profile  } = cli.flags;

  let config = {region};

  if (maxAttempts != null) config.maxAttempts = maxAttempts;
  if (profile) config.credentials = fromIni({ profile });
  if (endpoint) config.endpoint = endpoint;
  
  if (cli.flags.caFile) {
    console.log('Using self signed cert', cli.flags.caFile);

    const certs = [readFileSync(cli.flags.caFile)];
          
    const agent = new Agent({
      rejectUnauthorized: true,
      ca: certs
    });

    config.requestHandler = new NodeHttpHandler({
      httpAgent: agent,
      httpsAgent: agent
    })        
  } 

  return new DynamoDBClient(config);;
}

async function listTablesCli() {
  const tables = await listTables();
  if (tables.length === 0) {
    logger.info('No tables found');
    return;
  }
  logger.info(tables.join(' '));
}

function listTables() {
  const dynamoDb = createDynamoDb();

  const params = {};

  let tables = [];
  async function listTablesPaged() {
    const command = new ListTablesCommand(params);
    const data = await dynamoDb.send(command)
    tables = tables.concat(data.TableNames);
    if (data.LastEvaluatedTableName !== undefined) {
      params.ExclusiveStartTableName = data.LastEvaluatedTableName;
      return listTablesPaged();
    }

    return tables;
  };

  return listTablesPaged();
}

async function exportSchemaCli() {
  logger.info('Exporting schema for table', tableName);
  if (dryRun) return;

  if (!tableName) {
    logger.error('--table is requred')
    cli.showHelp();
  }

  return exportSchema(tableName, cli.flags.file)
}

async function exportAllSchemaCli() {
  return pMap(await listTables(), async (tableName) => {
    logger.info('Exporting schema for table', tableName);
    if (dryRun) return;
    await exportSchema(tableName, null);
  }, { concurrency: 1 });
}

async function exportSchema(tableName, file) {
  const dynamoDb = createDynamoDb();
  const command = new DescribeTableCommand({ TableName: tableName });

  const data = await dynamoDb.send(command)
  const table = data.Table;
  const file2 = file || sanitizeFilename(tableName + '.dynamoschema');

  return writeFile(file2, JSON.stringify(table, null, 2))
}

async function importSchemaCli() {
  const file = cli.flags.file;
  const waitForActive = cli.flags.waitForActive;

  if (!file) {
    logger.error('--file is requred')
    cli.showHelp();
  }

  logger.info('Importing schema for table', tableName, 'from file', file);

  const dynamoDb = createDynamoDb();

  async function doWaitForActive() {
    const retries = 60;
    for (let i = 0; i < retries; i += 1) {
      const command = new DescribeTableCommand({ TableName: tableName });
      const data = await dynamoDb.send(command);
      if (data.Table.TableStatus === 'ACTIVE') return;
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    throw new Error('Timed out');
  }

  const json = await readFile(file)
  const data = JSON.parse(json)
  if (tableName) data.TableName = tableName;

  filterTable(data);

  if (dryRun) return;
  await dynamoDb.createTable(data).promise()
  if (waitForActive) return doWaitForActive();
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
  delete table.SSEDescription;

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
    } else if (index.ProvisionedThroughput) { // https://github.com/mifi/dynamodump/issues/26
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

function getThroughput(defaultThroughput) {
  if (cli.flags.throughput == null) return defaultThroughput;
  if (Number.isInteger(cli.flags.throughput) && cli.flags.throughput > 0) {
    return cli.flags.throughput;
  } else {
    logger.error('--throughput must be a positive integer');
    cli.showHelp();
  }
}

async function importDataCli() {
  const file = cli.flags.file;

  if (!tableName) {
    logger.error('--table is required')
    cli.showHelp();
  }
  if (!file) {
    logger.error('--file is required')
    cli.showHelp();
  }

  const throughput = getThroughput(1);

  const dynamoDb = createDynamoDb();

  logger.info('Importing data for table', tableName, 'from file', file);
  if (dryRun) return;

  const readStream = createReadStream(file);
  const parseStream = JSONStream.parse('*');

  let n = 0;

  const logProgress = () => logger.debug('Imported', n, 'items');
  const logThrottled = throttle(logProgress, 5000, { trailing: false });

  readStream.pipe(parseStream)
    .on('data', async (data) => {
      debug('data');

      if (cli.flags.marshall) {
        data = marshall(data);
      }

      n++;

      if (n >= throughput) {
        parseStream.pause();
      }

      try {
        const command = new PutItemCommand({ TableName: tableName, Item: data });
        await dynamoDb.send(command);
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

async function exportDataCli() {
  if (!tableName) {
    logger.error('--table is required')
    cli.showHelp();
  }

  logger.info('Exporting data for table', tableName);
  if (dryRun) return;

  return exportData(tableName, cli.flags.file);
}

async function exportAllDataCli() {
  return pMap(await listTables(), async (tableName) => {
    logger.info('Exporting data for table', tableName);
    if (dryRun) return;
    await exportData(tableName, null);
  }, { concurrency: 1 });
}

async function exportData(tableName, file) {
  const dynamoDb = createDynamoDb();

  const file2 = file || sanitizeFilename(tableName + '.dynamodata');
  const writeStream = createWriteStream(file2);
  const stringify = JSONStream.stringify();

  let n = 0;

  const params = { TableName: tableName };

  async function scanPage() {
    const command = new ScanCommand({ TableName: tableName });
    const data = await dynamoDb.send(command);

    data.Items.forEach((item) => {
      if (cli.flags.marshall) {
        item = unmarshall(item);
      }
      return stringify.write(item)
    });

    n += data.Items.length;
    logger.debug('Exported', n, 'items');

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

async function exportAllCli() {
  return pMap(await listTables(), async (tableName) => {
    logger.info('Exporting schema and data for table', tableName);
    if (dryRun) return;
    await exportSchema(tableName, null);
    await exportData(tableName, null);
  }, { concurrency: 1 });
}

async function wipeDataCli() {
  if (!tableName) {
    logger.error('--table is required')
    cli.showHelp();
  }

  logger.info('Wiping data for table', tableName);
  if (dryRun) return;

  return wipeData(tableName, getThroughput(10));
}

async function wipeData(tableName, throughput) {
  const dynamoDb = createDynamoDb();

  let n = 0;

  const params = {
    TableName: tableName,
    Limit: throughput
  };

  async function scanPage(keyFields) {
    const command = new ScanCommand({ TableName: tableName });
    const data = await dynamoDb.send(command)

    await pMap(data.Items, (item) => {
      const delParams = {
        TableName: tableName,
        Key: pick(item, keyFields)
      };
      const delCommand = new DeleteItemCommand(delParams);
      return dynamoDb.send(delCommand);
    }, { concurrency: 10 })

    n += data.Items.length;
    logger.debug('Wiped', n, 'items');

    if (data.LastEvaluatedKey !== undefined) {
      params.ExclusiveStartKey = data.LastEvaluatedKey;
      return scanPage(keyFields);
    }
  }

  const descCommand = new DescribeTableCommand({ TableName: tableName });
  const table = await dynamoDb.send(descCommand);;
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
  await method()
} catch (err) {
  if (cli.flags.stackTrace) {
    logger.error('Error:', err);
  } else {
    logger.error('Error:', err.message);
  }
  process.exitCode = 1;
}
