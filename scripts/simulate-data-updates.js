const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
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
const NUM_NEW_RECORDS = 500;
const NUM_PARTITIONS = 5;

function generateNewSampleData(numRecords, startId) {
  const data = [];
  for (let i = 0; i < numRecords; i++) {
    const id = startId + i;
    data.push({
      id: id,
      name: `User ${id}`,
      email: `user${id}@example.com`,
      age: Math.floor(Math.random() * 80) + 18,
      active: Math.random() > 0.3,
      created_at: new Date().toISOString(), // New records have current timestamp
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

async function getJsonFromS3(s3Key) {
  const command = new GetObjectCommand({
    Bucket: BUCKET_NAME,
    Key: s3Key
  });
  
  const response = await s3Client.send(command);
  const stream = response.Body;
  
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  const buffer = Buffer.concat(chunks);
  const jsonString = buffer.toString('utf-8');
  
  return JSON.parse(jsonString);
}

async function createNewCsvFiles(data) {
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
    const fileName = `data_part_${partition}_update.csv`;
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

async function updateIcebergMetadata(newDataFiles) {
  const currentTimestamp = Date.now();
  
  const currentMetadata = await getJsonFromS3(`${TABLE_LOCATION}/metadata/v1.metadata.json`);
  
  const currentSnapshotId = currentMetadata.current_snapshot_id;
  const currentSnapshot = currentMetadata.snapshots.find(s => s.snapshot_id === currentSnapshotId);
  
  const currentManifestList = await getJsonFromS3(currentSnapshot.manifest_list);
  
  const currentManifestPath = currentManifestList.manifests[0].manifest_path;
  const currentManifest = await getJsonFromS3(currentManifestPath);
  
  const newSnapshotId = currentSnapshotId + 1;
  
  const newManifestEntries = newDataFiles.map((file, index) => ({
    status: 1, // 1 = added
    snapshot_id: newSnapshotId,
    sequence_number: currentMetadata.last_sequence_number + 1,
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
  
  const allManifestEntries = [...currentManifest.entries, ...newManifestEntries];
  
  const newManifestPath = `${TABLE_LOCATION}/metadata/manifests/manifest-${newSnapshotId}.json`;
  await uploadJsonToS3({ entries: allManifestEntries }, newManifestPath);
  
  const newManifestList = {
    manifests: [{
      manifest_path: newManifestPath,
      manifest_length: JSON.stringify(allManifestEntries).length,
      partition_spec_id: 0,
      content: 0, // 0 = data
      sequence_number: currentMetadata.last_sequence_number + 1,
      min_sequence_number: 1,
      added_snapshot_id: newSnapshotId,
      added_files_count: newDataFiles.length,
      existing_files_count: currentManifestList.manifests[0].added_files_count,
      deleted_files_count: 0,
      added_rows_count: newDataFiles.reduce((sum, file) => sum + file.recordCount, 0),
      existing_rows_count: currentManifestList.manifests[0].added_rows_count,
      deleted_rows_count: 0,
      partitions: [],
      key_metadata: null
    }]
  };
  
  const newManifestListPath = `${TABLE_LOCATION}/metadata/snap-${newSnapshotId}-${currentMetadata.last_sequence_number + 1}-${currentTimestamp}.json`;
  await uploadJsonToS3(newManifestList, newManifestListPath);
  
  const newSnapshot = {
    snapshot_id: newSnapshotId,
    parent_snapshot_id: currentSnapshotId,
    sequence_number: currentMetadata.last_sequence_number + 1,
    timestamp_ms: currentTimestamp,
    manifest_list: newManifestListPath,
    summary: {
      operation: 'append',
      'app.id': 'iceberg-duckdb-wasm-update'
    },
    schema_id: currentMetadata.current_schema_id
  };
  
  const newMetadata = {
    ...currentMetadata,
    last_sequence_number: currentMetadata.last_sequence_number + 1,
    last_updated_ms: currentTimestamp,
    current_snapshot_id: newSnapshotId,
    snapshots: [...currentMetadata.snapshots, newSnapshot],
    snapshot_log: [
      ...currentMetadata.snapshot_log,
      {
        timestamp_ms: currentTimestamp,
        snapshot_id: newSnapshotId
      }
    ],
    metadata_log: [
      ...currentMetadata.metadata_log,
      {
        timestamp_ms: currentTimestamp,
        metadata_file: `${TABLE_LOCATION}/metadata/v2.metadata.json`
      }
    ]
  };
  
  const newMetadataPath = `${TABLE_LOCATION}/metadata/v2.metadata.json`;
  await uploadJsonToS3(newMetadata, newMetadataPath);
  
  return {
    newMetadata,
    newMetadataPath
  };
}

async function simulateDataUpdates() {
  try {
    console.log('Getting current metadata...');
    const currentMetadata = await getJsonFromS3(`${TABLE_LOCATION}/metadata/v1.metadata.json`);
    
    const startId = 1000; // Assuming the original data had 1000 records with IDs 0-999
    
    console.log('Generating new sample data...');
    const newData = generateNewSampleData(NUM_NEW_RECORDS, startId);
    
    console.log('Creating new CSV files...');
    const newCsvFiles = await createNewCsvFiles(newData);
    
    console.log('Updating Iceberg metadata...');
    const { newMetadataPath } = await updateIcebergMetadata(newCsvFiles);
    
    console.log('Iceberg data update simulation complete!');
    console.log(`New metadata file: ${newMetadataPath}`);
    console.log(`Added ${newCsvFiles.length} new CSV files with ${NUM_NEW_RECORDS} total new records`);
    console.log('The application should now be able to detect and process these changes');
    
  } catch (error) {
    console.error('Error simulating data updates:', error);
  }
}

simulateDataUpdates();
