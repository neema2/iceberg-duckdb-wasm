const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

const s3Client = new S3Client({
  endpoint: 'http://localhost:9000',
  region: 'us-east-1',
  forcePathStyle: true,
  credentials: {
    accessKeyId: 'minioadmin',
    secretAccessKey: 'minioadmin'
  }
});

const BUCKET_NAME = 'iceberg-data';
const TABLE_NAME = 'my-table';
const TABLE_LOCATION = `${TABLE_NAME}`;
const NUM_RECORDS = 1000;
const NUM_PARTITIONS = 5;

function generateSampleData(numRecords) {
  const data = [];
  for (let i = 0; i < numRecords; i++) {
    data.push({
      id: i,
      name: `User ${i}`,
      email: `user${i}@example.com`,
      age: Math.floor(Math.random() * 80) + 18,
      active: Math.random() > 0.3,
      created_at: new Date(Date.now() - Math.floor(Math.random() * 10000000000)).toISOString(),
      score: parseFloat((Math.random() * 100).toFixed(2)),
      department: ['Engineering', 'Marketing', 'Sales', 'Support', 'HR'][Math.floor(Math.random() * 5)],
      partition: Math.floor(i / (numRecords / NUM_PARTITIONS))
    });
  }
  return data;
}

async function uploadToS3(filePath, s3Key) {
  const fileContent = fs.readFileSync(filePath);
  
  const command = new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: s3Key,
    Body: fileContent
  });
  
  await s3Client.send(command);
  console.log(`Uploaded ${s3Key} to S3`);
  
  return s3Key;
}

async function uploadJsonToS3(data, s3Key) {
  const jsonContent = JSON.stringify(data, null, 2);
  
  const command = new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: s3Key,
    Body: jsonContent
  });
  
  await s3Client.send(command);
  console.log(`Uploaded JSON to ${s3Key}`);
  
  return s3Key;
}

async function createIcebergMetadata(dataFiles, schemaFields) {
  const tableUuid = uuidv4();
  const currentTimestamp = Date.now();
  
  const schema = {
    type: 'struct',
    schema_id: 0,
    fields: schemaFields.map((field, index) => ({
      id: index,
      name: field.name,
      required: false,
      type: field.type
    }))
  };
  
  const manifestEntries = dataFiles.map((file, index) => ({
    status: 1, // 1 = added
    snapshot_id: 1,
    sequence_number: 1,
    file_sequence_number: index + 1,
    data_file: {
      content: 0, // 0 = data
      file_path: file.s3Key,
      file_format: 'PARQUET',
      partition: { partition: file.partition },
      record_count: file.recordCount,
      file_size_in_bytes: file.fileSize,
      column_sizes: {},
      value_counts: {},
      null_value_counts: {},
      nan_value_counts: {},
      lower_bounds: {},
      upper_bounds: {},
      key_metadata: null,
      split_offsets: [],
      sort_order_id: 0
    }
  }));
  
  const manifestPath = `${TABLE_LOCATION}/metadata/manifests/manifest-1.json`;
  await uploadJsonToS3({ entries: manifestEntries }, manifestPath);
  
  const manifestList = {
    manifests: [{
      manifest_path: manifestPath,
      manifest_length: JSON.stringify(manifestEntries).length,
      partition_spec_id: 0,
      content: 0, // 0 = data
      sequence_number: 1,
      min_sequence_number: 1,
      added_snapshot_id: 1,
      added_files_count: dataFiles.length,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: dataFiles.reduce((sum, file) => sum + file.recordCount, 0),
      existing_rows_count: 0,
      deleted_rows_count: 0,
      partitions: [],
      key_metadata: null
    }]
  };
  
  const manifestListPath = `${TABLE_LOCATION}/metadata/snap-1-1-${currentTimestamp}.json`;
  await uploadJsonToS3(manifestList, manifestListPath);
  
  const snapshot = {
    snapshot_id: 1,
    parent_snapshot_id: null,
    sequence_number: 1,
    timestamp_ms: currentTimestamp,
    manifest_list: manifestListPath,
    summary: {
      operation: 'append',
      'app.id': 'iceberg-duckdb-wasm'
    },
    schema_id: 0
  };
  
  const tableMetadata = {
    format_version: 2,
    table_uuid: tableUuid,
    location: TABLE_LOCATION,
    last_sequence_number: 1,
    last_updated_ms: currentTimestamp,
    last_column_id: schemaFields.length - 1,
    schemas: [schema],
    current_schema_id: 0,
    partition_specs: [{
      spec_id: 0,
      fields: [{
        name: 'partition',
        transform: 'identity',
        source_id: schemaFields.findIndex(f => f.name === 'partition'),
        field_id: schemaFields.length
      }]
    }],
    default_spec_id: 0,
    last_partition_id: schemaFields.length,
    properties: {},
    current_snapshot_id: 1,
    snapshots: [snapshot],
    snapshot_log: [{
      timestamp_ms: currentTimestamp,
      snapshot_id: 1
    }],
    metadata_log: [{
      timestamp_ms: currentTimestamp,
      metadata_file: `${TABLE_LOCATION}/metadata/v1.metadata.json`
    }],
    sort_orders: [{
      order_id: 0,
      fields: []
    }],
    default_sort_order_id: 0,
    refs: {
      main: {
        snapshot_id: 1,
        type: 'branch'
      }
    }
  };
  
  const metadataPath = `${TABLE_LOCATION}/metadata/v1.metadata.json`;
  await uploadJsonToS3(tableMetadata, metadataPath);
  
  return {
    tableMetadata,
    metadataPath
  };
}

async function createCsvFiles(data) {
  const partitionedData = {};
  
  data.forEach(record => {
    const partition = record.partition;
    if (!partitionedData[partition]) {
      partitionedData[partition] = [];
    }
    partitionedData[partition].push(record);
  });
  
  const csvFiles = [];
  
  for (const partition in partitionedData) {
    const records = partitionedData[partition];
    const fileName = `data_part_${partition}.csv`;
    const filePath = path.join(__dirname, fileName);
    
    const header = Object.keys(records[0]).join(',');
    
    const rows = records.map(record => {
      return Object.values(record).map(value => {
        if (typeof value === 'string') {
          return `"${value}"`;
        }
        return value;
      }).join(',');
    });
    
    const csvContent = [header, ...rows].join('\n');
    fs.writeFileSync(filePath, csvContent);
    
    const s3Key = `${TABLE_LOCATION}/data/${fileName}`;
    await uploadToS3(filePath, s3Key);
    
    csvFiles.push({
      s3Key,
      partition: parseInt(partition),
      recordCount: records.length,
      fileSize: fs.statSync(filePath).size
    });
    
    fs.unlinkSync(filePath);
  }
  
  return csvFiles;
}

async function generateIcebergData() {
  try {
    console.log('Generating sample data...');
    const allData = generateSampleData(NUM_RECORDS);
    
    console.log('Creating CSV files...');
    const csvFiles = await createCsvFiles(allData);
    
    console.log('Creating Iceberg metadata...');
    const schemaFields = [
      { name: 'id', type: 'long' },
      { name: 'name', type: 'string' },
      { name: 'email', type: 'string' },
      { name: 'age', type: 'int' },
      { name: 'active', type: 'boolean' },
      { name: 'created_at', type: 'string' },
      { name: 'score', type: 'double' },
      { name: 'department', type: 'string' },
      { name: 'partition', type: 'int' }
    ];
    
    const { metadataPath } = await createIcebergMetadata(csvFiles, schemaFields);
    
    console.log('Iceberg data generation complete!');
    console.log(`Table location: ${TABLE_LOCATION}`);
    console.log(`Metadata file: ${metadataPath}`);
    console.log(`Created ${csvFiles.length} CSV files with ${NUM_RECORDS} total records`);
    
  } catch (error) {
    console.error('Error generating Iceberg data:', error);
  }
}

generateIcebergData();
