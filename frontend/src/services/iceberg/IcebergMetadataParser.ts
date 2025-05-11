import s3Client from '../s3/S3Client';

interface IcebergManifestEntry {
  status: number;
  snapshot_id: number;
  sequence_number: number;
  file_sequence_number: number;
  data_file: {
    content: number;
    file_path: string;
    file_format: string;
    partition: Record<string, any>;
    record_count: number;
    file_size_in_bytes: number;
    column_sizes: Record<number, number>;
    value_counts: Record<number, number>;
    null_value_counts: Record<number, number>;
    nan_value_counts: Record<number, number>;
    lower_bounds: Record<number, any>;
    upper_bounds: Record<number, any>;
    key_metadata: any;
    split_offsets: number[];
    sort_order_id: number;
  };
}

interface IcebergManifestFile {
  manifest_path: string;
  manifest_length: number;
  partition_spec_id: number;
  content: number;
  sequence_number: number;
  min_sequence_number: number;
  added_snapshot_id: number;
  added_files_count: number;
  existing_files_count: number;
  deleted_files_count: number;
  added_rows_count: number;
  existing_rows_count: number;
  deleted_rows_count: number;
  partitions: any[];
  key_metadata: any;
}

interface IcebergSnapshot {
  snapshot_id: number;
  parent_snapshot_id: number | null;
  sequence_number: number;
  timestamp_ms: number;
  manifest_list: string;
  summary: {
    operation: string;
    [key: string]: any;
  };
  schema_id: number;
}

interface IcebergTableMetadata {
  format_version: number;
  table_uuid: string;
  location: string;
  last_sequence_number: number;
  last_updated_ms: number;
  last_column_id: number;
  schemas: any[];
  current_schema_id: number;
  partition_specs: any[];
  default_spec_id: number;
  last_partition_id: number;
  properties: Record<string, string>;
  current_snapshot_id: number;
  snapshots: IcebergSnapshot[];
  snapshot_log: { timestamp_ms: number; snapshot_id: number }[];
  metadata_log: { timestamp_ms: number; metadata_file: string }[];
  sort_orders: any[];
  default_sort_order_id: number;
  refs: Record<string, { snapshot_id: number; type: string }>;
}

class IcebergMetadataParser {
  private tableLocation: string;
  private s3Client = s3Client;
  private tableMetadata: IcebergTableMetadata | null = null;
  private parquetFiles: string[] = [];

  constructor(tableLocation: string) {
    this.tableLocation = tableLocation;
  }

  /**
   * Initialize the parser by loading the table metadata
   */
  async initialize(): Promise<void> {
    try {
      const metadataFiles = await this.s3Client.listObjects(`${this.tableLocation}/metadata/`);
      
      const versionRegex = /v(\d+)\.metadata\.json$/;
      const metadataVersions = metadataFiles
        .filter(file => versionRegex.test(file))
        .sort((a, b) => {
          const versionA = parseInt(a.match(versionRegex)?.[1] || '0');
          const versionB = parseInt(b.match(versionRegex)?.[1] || '0');
          return versionB - versionA; // Sort in descending order
        });
      
      if (metadataVersions.length === 0) {
        throw new Error(`No metadata files found at ${this.tableLocation}/metadata/`);
      }
      
      const latestMetadataFile = metadataVersions[0];
      console.log(`Using latest Iceberg metadata file: ${latestMetadataFile}`);
      
      this.tableMetadata = await this.s3Client.getObjectAsJson<IcebergTableMetadata>(latestMetadataFile);
      
      await this.extractParquetFilesFromCurrentSnapshot();
    } catch (error) {
      console.error('Error initializing Iceberg metadata parser:', error);
      throw error;
    }
  }

  /**
   * Extract Parquet file paths from the current snapshot
   */
  private async extractParquetFilesFromCurrentSnapshot(): Promise<void> {
    if (!this.tableMetadata) {
      throw new Error('Table metadata not loaded');
    }

    try {
      const currentSnapshotId = this.tableMetadata.current_snapshot_id;
      
      const currentSnapshot = this.tableMetadata.snapshots.find(
        (snapshot) => snapshot.snapshot_id === currentSnapshotId
      );

      if (!currentSnapshot) {
        throw new Error(`Current snapshot (ID: ${currentSnapshotId}) not found`);
      }

      console.log(`Processing snapshot ID: ${currentSnapshotId}, timestamp: ${new Date(currentSnapshot.timestamp_ms).toISOString()}`);
      
      // Get the manifest list path from the current snapshot
      const manifestListPath = currentSnapshot.manifest_list;
      console.log(`Manifest list path: ${manifestListPath}`);
      
      let manifestList;
      try {
        manifestList = await this.s3Client.getObjectAsJson<{ manifests: IcebergManifestFile[] }>(manifestListPath);
      } catch (error: any) {
        console.error(`Error loading manifest list from ${manifestListPath}:`, error);
        throw new Error(`Failed to load manifest list: ${error?.message || 'Unknown error'}`);
      }
      
      if (!manifestList || !manifestList.manifests || !Array.isArray(manifestList.manifests)) {
        throw new Error(`Invalid manifest list format at ${manifestListPath}`);
      }
      
      console.log(`Found ${manifestList.manifests.length} manifest files`);
      
      for (const manifestFile of manifestList.manifests) {
        try {
          console.log(`Processing manifest: ${manifestFile.manifest_path}`);
          
          const manifestEntries = await this.s3Client.getObjectAsJson<{ entries: IcebergManifestEntry[] }>(
            manifestFile.manifest_path
          );
          
          if (!manifestEntries || !manifestEntries.entries || !Array.isArray(manifestEntries.entries)) {
            console.warn(`Invalid manifest entries format at ${manifestFile.manifest_path}, skipping`);
            continue;
          }
          
          for (const entry of manifestEntries.entries) {
            if (entry.status !== 2) {
              if (!entry.data_file || !entry.data_file.file_path) {
                console.warn('Manifest entry missing data_file or file_path, skipping');
                continue;
              }
              
              this.parquetFiles.push(entry.data_file.file_path);
            }
          }
          
          console.log(`Added ${manifestEntries.entries.length} Parquet files from manifest`);
        } catch (error) {
          console.error(`Error processing manifest ${manifestFile.manifest_path}:`, error);
        }
      }
      
      console.log(`Total Parquet files found: ${this.parquetFiles.length}`);
    } catch (error) {
      console.error('Error extracting Parquet files from current snapshot:', error);
      throw error;
    }
  }

  /**
   * Get all Parquet file paths from the current snapshot
   */
  getParquetFiles(): string[] {
    return this.parquetFiles;
  }

  /**
   * Get table schema from metadata
   */
  getTableSchema(): any {
    if (!this.tableMetadata) {
      throw new Error('Table metadata not loaded');
    }

    const currentSchemaId = this.tableMetadata.current_schema_id;
    return this.tableMetadata.schemas.find((schema) => schema.schema_id === currentSchemaId);
  }

  /**
   * Check if the table metadata has been updated
   * @param lastUpdatedMs Last known update timestamp
   */
  async checkForUpdates(lastUpdatedMs: number): Promise<boolean> {
    try {
      const metadataFiles = await this.s3Client.listObjects(`${this.tableLocation}/metadata/`);
      
      const versionRegex = /v(\d+)\.metadata\.json$/;
      const metadataVersions = metadataFiles
        .filter(file => versionRegex.test(file))
        .sort((a, b) => {
          const versionA = parseInt(a.match(versionRegex)?.[1] || '0');
          const versionB = parseInt(b.match(versionRegex)?.[1] || '0');
          return versionB - versionA; // Sort in descending order
        });
      
      if (metadataVersions.length === 0) {
        console.warn(`No metadata files found at ${this.tableLocation}/metadata/`);
        return false;
      }
      
      const latestMetadataFile = metadataVersions[0];
      
      // Load the metadata and check the last_updated_ms timestamp
      const metadata = await this.s3Client.getObjectAsJson<IcebergTableMetadata>(latestMetadataFile);
      
      return metadata.last_updated_ms > lastUpdatedMs;
    } catch (error) {
      console.error('Error checking for updates:', error);
      return false;
    }
  }

  /**
   * Reload metadata and update Parquet file list
   */
  async refresh(): Promise<void> {
    this.parquetFiles = [];
    await this.initialize();
  }
}

export default IcebergMetadataParser;
