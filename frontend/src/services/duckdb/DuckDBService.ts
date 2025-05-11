import { AsyncDuckDB, AsyncDuckDBConnection, ConsoleLogger, selectBundle } from '@duckdb/duckdb-wasm';
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
      
      const logger = new ConsoleLogger();
      
      const bundles = {
        mvp: {
          mainModule: new URL('/node_modules/@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm', import.meta.url).toString(),
          mainWorker: new URL('/node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js', import.meta.url).toString(),
        },
        eh: {
          mainModule: new URL('/node_modules/@duckdb/duckdb-wasm/dist/duckdb-eh.wasm', import.meta.url).toString(),
          mainWorker: new URL('/node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js', import.meta.url).toString(),
        }
      };
      
      console.log('Selecting DuckDB bundle based on browser capabilities...');
      
      const bundle = await selectBundle(bundles);
      
      console.log('Selected bundle:', bundle);
      
      this.db = new AsyncDuckDB(logger);
      
      console.log('Instantiating DuckDB...');
      await this.db.instantiate(bundle.mainModule, bundle.mainWorker);
      
      console.log('Connecting to DuckDB...');
      this.conn = await this.db.connect();
      
      const versionResult = await this.conn.query(`SELECT version() AS version;`);
      console.log('DuckDB Version:', versionResult.toArray()[0].version);
      
      console.log('Loading HTTPFS extension...');
      try {
        await this.conn.query(`LOAD httpfs;`);
        console.log('HTTPFS extension loaded successfully');
      } catch (e) {
        console.log('HTTPFS not available, installing it first...');
        await this.conn.query(`INSTALL httpfs;`);
        await this.conn.query(`LOAD httpfs;`);
        console.log('HTTPFS extension installed and loaded successfully');
      }
      
      console.log('Configuring S3 settings...');
      await this.conn.query(`
        SET s3_region='us-east-1';
        SET s3_endpoint='localhost:9000';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin';
      `);
      
      console.log('Testing DuckDB connection...');
      const testResult = await this.conn.query(`SELECT 1 AS test;`);
      console.log('Test query result:', testResult.toArray());
      
      console.log('DuckDB-WASM initialized successfully');
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
      console.log('Checking for Iceberg metadata updates...');
      const hasUpdates = await this.icebergParser.checkForUpdates(this.lastUpdatedMs);
      
      if (hasUpdates || this.lastUpdatedMs === 0) {
        console.log('Refreshing Iceberg metadata...');
        
        if (hasUpdates) {
          await this.icebergParser.refresh();
        }
        
        this.lastUpdatedMs = Date.now();
        
        const parquetFiles = this.icebergParser.getParquetFiles();
        console.log(`Found ${parquetFiles.length} Parquet files in Iceberg metadata`);
        
        if (parquetFiles.length > 0) {
          await this.conn.query(`DROP VIEW IF EXISTS iceberg_data;`);
          
          await this.conn.query(`
            SET s3_region='us-east-1';
            SET s3_endpoint='localhost:9000';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
          `);
          
          const s3Files = parquetFiles.map(file => {
            if (file.startsWith('s3://')) {
              return `'${file}'`;
            }
            return `'s3://iceberg-data/${file}'`;
          });
          
          const filesStr = s3Files.join(', ');
          console.log('Creating view for data files...');
          
          const isCSV = parquetFiles.some(file => file.toLowerCase().endsWith('.csv'));
          
          if (isCSV) {
            console.log('Using CSV files...');
            await this.conn.query(`
              CREATE VIEW iceberg_data AS 
              SELECT * FROM read_csv_auto([${filesStr}]);
            `);
          } else {
            console.log('Using Parquet files...');
            await this.conn.query(`
              CREATE VIEW iceberg_data AS 
              SELECT * FROM parquet_scan([${filesStr}]);
            `);
          }
          
          console.log('View created successfully');
          return true;
        } else {
          console.warn('No Parquet files found in Iceberg metadata');
        }
      } else {
        console.log('No updates to Iceberg metadata');
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
  /**
   * Execute a SQL query against the Iceberg data
   * @param query SQL query to execute
   * @param forceRefresh Whether to force a refresh of the Parquet files
   * @returns Query results as an array of objects
   */
  async executeQuery(query: string, forceRefresh: boolean = false): Promise<any[]> {
    if (!this.initialized || !this.conn) {
      await this.initialize();
      
      if (!this.initialized || !this.conn) {
        throw new Error('Failed to initialize DuckDB');
      }
    }

    try {
      console.log(`Executing query: ${query}`);
      
      if (forceRefresh) {
        console.log('Forcing refresh of Parquet files');
        await this.refreshParquetFiles();
      } else {
        const refreshed = await this.refreshParquetFiles();
        if (refreshed) {
          console.log('Parquet files were refreshed due to updates');
        }
      }
      
      console.log('Running query against DuckDB...');
      const result = await this.conn!.query(query);
      
      const resultArray = result.toArray();
      console.log(`Query returned ${resultArray.length} rows`);
      
      return resultArray;
    } catch (error: any) {
      console.error('Error executing query:', error);
      
      if (error.message && error.message.includes('not found')) {
        throw new Error(`Table or view not found. Make sure you've loaded Iceberg data: ${error.message}`);
      } else if (error.message && error.message.includes('syntax error')) {
        throw new Error(`SQL syntax error: ${error.message}`);
      } else {
        throw new Error(`Error executing query: ${error.message || 'Unknown error'}`);
      }
    }
  }

  /**
   * Get the schema of the Iceberg table
   */
  /**
   * Get the schema of the Iceberg table
   * @returns Table schema information as an array of column definitions
   */
  async getTableSchema(): Promise<any[]> {
    if (!this.initialized || !this.conn) {
      await this.initialize();
      
      if (!this.initialized || !this.conn) {
        throw new Error('Failed to initialize DuckDB');
      }
    }

    try {
      console.log('Getting table schema...');
      
      const viewExists = await this.tableExists('iceberg_data');
      if (!viewExists) {
        throw new Error('Iceberg data view does not exist. Make sure you have loaded Iceberg data first.');
      }
      
      const result = await this.conn!.query(`DESCRIBE iceberg_data;`);
      const schema = result.toArray();
      
      console.log(`Schema has ${schema.length} columns`);
      return schema;
    } catch (error: any) {
      console.error('Error getting table schema:', error);
      throw new Error(`Error getting table schema: ${error.message || 'Unknown error'}`);
    }
  }
  
  /**
   * Check if a table or view exists in DuckDB
   * @param tableName Name of the table or view to check
   * @returns True if the table or view exists, false otherwise
   */
  async tableExists(tableName: string): Promise<boolean> {
    if (!this.initialized || !this.conn) {
      return false;
    }
    
    try {
      const result = await this.conn!.query(`
        SELECT COUNT(*) as count 
        FROM information_schema.tables 
        WHERE table_name = '${tableName}'
      `);
      
      const count = result.toArray()[0].count;
      return count > 0;
    } catch (error) {
      console.error(`Error checking if table ${tableName} exists:`, error);
      return false;
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
