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
      const metadataPath = `${this.tableLocation}/metadata/v2.metadata.json`;
      this.tableMetadata = await this.s3Client.getObjectAsJson<IcebergTableMetadata>(metadataPath);
      
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

      const manifestListPath = currentSnapshot.manifest_list;
      const manifestList = await this.s3Client.getObjectAsJson<{ manifests: IcebergManifestFile[] }>(manifestListPath);

      for (const manifestFile of manifestList.manifests) {
        const manifestEntries = await this.s3Client.getObjectAsJson<{ entries: IcebergManifestEntry[] }>(
          manifestFile.manifest_path
        );

        for (const entry of manifestEntries.entries) {
          if (entry.status !== 2) { // Not deleted
            this.parquetFiles.push(entry.data_file.file_path);
          }
        }
      }
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
      const metadataPath = `${this.tableLocation}/metadata/v2.metadata.json`;
      const metadata = await this.s3Client.getObjectAsJson<IcebergTableMetadata>(metadataPath);
      
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
