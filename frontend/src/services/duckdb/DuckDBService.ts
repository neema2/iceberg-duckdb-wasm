import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import IcebergMetadataParser from '../iceberg/IcebergMetadataParser';

class DuckDBService {
  private db: AsyncDuckDB | null = null;
  private conn: AsyncDuckDBConnection | null = null;
  private initialized = false;
  private icebergParser: IcebergMetadataParser | null = null;
  private lastUpdatedMs = 0;

  /**
   * Initialize DuckDB and load the HTTPFS extension for S3 access
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

    try {
      console.log('Initializing DuckDB-WASM...');
      
      this.initialized = true;
    } catch (error) {
      console.error('Error initializing DuckDB:', error);
      throw error;
    }
  }

  /**
   * Set up the Iceberg table by loading Parquet files from S3
   * @param tableLocation S3 location of the Iceberg table
   */
  async setupIcebergTable(tableLocation: string): Promise<void> {
    if (!this.initialized || !this.conn) {
      await this.initialize();
    }

    try {
      this.icebergParser = new IcebergMetadataParser(tableLocation);
      await this.icebergParser.initialize();
      
      this.icebergParser.getTableSchema();
      this.lastUpdatedMs = Date.now();
      
      await this.refreshParquetFiles();
    } catch (error) {
      console.error('Error setting up Iceberg table:', error);
      throw error;
    }
  }

  /**
   * Refresh the Parquet files if the Iceberg metadata has been updated
   */
  async refreshParquetFiles(): Promise<boolean> {
    if (!this.icebergParser || !this.conn) {
      throw new Error('Iceberg parser or DuckDB connection not initialized');
    }

    try {
      const hasUpdates = await this.icebergParser.checkForUpdates(this.lastUpdatedMs);
      
      if (hasUpdates) {
        await this.icebergParser.refresh();
        this.lastUpdatedMs = Date.now();
        
        const parquetFiles = this.icebergParser.getParquetFiles();
        
        if (parquetFiles.length > 0) {
          await this.conn.query(`DROP VIEW IF EXISTS iceberg_data;`);
          
          const parquetFilesStr = parquetFiles.map(file => `'${file}'`).join(', ');
          await this.conn.query(`
            CREATE VIEW iceberg_data AS 
            SELECT * FROM parquet_scan([${parquetFilesStr}]);
          `);
          
          return true;
        }
      }
      
      return false;
    } catch (error) {
      console.error('Error refreshing Parquet files:', error);
      throw error;
    }
  }

  /**
   * Execute a SQL query against the Iceberg data
   * @param query SQL query to execute
   */
  async executeQuery(query: string): Promise<any[]> {
    if (!this.initialized || !this.conn) {
      throw new Error('DuckDB not initialized');
    }

    try {
      await this.refreshParquetFiles();
      
      const result = await this.conn.query(query);
      return result.toArray();
    } catch (error) {
      console.error('Error executing query:', error);
      throw error;
    }
  }

  /**
   * Get the schema of the Iceberg table
   */
  async getTableSchema(): Promise<any[]> {
    if (!this.initialized || !this.conn) {
      throw new Error('DuckDB not initialized');
    }

    try {
      const result = await this.conn.query(`DESCRIBE iceberg_data;`);
      return result.toArray();
    } catch (error) {
      console.error('Error getting table schema:', error);
      throw error;
    }
  }

  /**
   * Close the DuckDB connection and release resources
   */
  async close(): Promise<void> {
    if (this.conn) {
      await this.conn.close();
      this.conn = null;
    }
    
    if (this.db) {
      await this.db.terminate();
      this.db = null;
    }
    
    this.initialized = false;
  }
}

const duckDBService = new DuckDBService();

export default duckDBService;
