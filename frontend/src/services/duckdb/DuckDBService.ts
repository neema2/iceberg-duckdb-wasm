import { AsyncDuckDB, AsyncDuckDBConnection, ConsoleLogger, selectBundle } from '@duckdb/duckdb-wasm';
import IcebergMetadataParser from '../iceberg/IcebergMetadataParser';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';

class DuckDBService {
  private db: AsyncDuckDB | null = null;
  private conn: AsyncDuckDBConnection | null = null;
  private initialized = false;
  private icebergParser: IcebergMetadataParser | null = null;
  private lastUpdatedMs = 0;
  private initializationPromise: Promise<void> | null = null;

  /**
   * Initialize DuckDB and load the HTTPFS extension for S3 access
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;
    
    if (this.initializationPromise) {
      return this.initializationPromise;
    }
    
    this.initializationPromise = this._initialize();
    return this.initializationPromise;
  }
  
  private async _initialize(): Promise<void> {
    try {
      console.log('Initializing DuckDB-WASM with CDN bundles...');
      
      const JSDELIVR_BUNDLES = {
        mvp: {
          mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-mvp.wasm',
          mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-mvp.worker.js',
        },
        eh: {
          mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-eh.wasm',
          mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-eh.worker.js',
        },
      };
      
      const bundle = await selectBundle(JSDELIVR_BUNDLES);
      
      const logger = new ConsoleLogger();
      this.db = new AsyncDuckDB(logger);
      
      console.log('Instantiating DuckDB with bundle:', bundle.mainModule);
      await this.db.instantiate(bundle.mainModule, bundle.mainWorker);
      console.log('DuckDB instantiated successfully');
      
      console.log('Connecting to DuckDB...');
      this.conn = await this.db.connect();
      console.log('Connected to DuckDB');
      
      try {
        console.log('Testing connection with a simple query...');
        const result = await this.conn.query('SELECT 1 AS test');
        console.log('Connection test result:', result.toArray());
      } catch (e) {
        console.error('Connection test failed:', e);
        throw new Error('Failed to verify DuckDB connection');
      }
      
      this.initialized = true;
      console.log('DuckDB-WASM initialized successfully');
    } catch (error) {
      console.error('Error initializing DuckDB:', error);
      this.initialized = false;
      this.db = null;
      this.conn = null;
      this.initializationPromise = null;
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
      
      this.lastUpdatedMs = Date.now();
      
      await this.refreshParquetFiles();
    } catch (error) {
      console.error('Error setting up Iceberg table:', error);
      throw error;
    }
  }

  /**
   * Refresh the Parquet files if the Iceberg metadata has been updated
   * Uses direct S3 access to fetch data files and register them with DuckDB
   */
  async refreshParquetFiles(): Promise<boolean> {
    if (!this.icebergParser || !this.conn || !this.db) {
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
        
        const dataFiles = this.icebergParser.getParquetFiles();
        console.log(`Found ${dataFiles.length} data files in Iceberg metadata`);
        
        if (dataFiles.length > 0) {
          const s3Client = new S3Client({
            endpoint: 'http://localhost:9000',
            region: 'us-east-1',
            forcePathStyle: true,
            credentials: {
              accessKeyId: 'minioadmin',
              secretAccessKey: 'minioadmin'
            }
          });
          
          await this.conn.query(`DROP VIEW IF EXISTS iceberg_data;`);
          
          await this.conn.query(`CREATE TABLE IF NOT EXISTS temp_iceberg_data (
            id INTEGER,
            name VARCHAR,
            email VARCHAR,
            age INTEGER,
            active BOOLEAN,
            created_at VARCHAR,
            score DOUBLE,
            department VARCHAR,
            partition INTEGER
          );`);
          
          await this.conn.query(`DELETE FROM temp_iceberg_data;`);
          
          for (const file of dataFiles) {
            console.log(`Processing file: ${file}`);
            
            try {
              let bucket = 'iceberg-data';
              let key = file;
              
              if (file.startsWith('s3://')) {
                const parts = file.replace('s3://', '').split('/');
                bucket = parts[0];
                key = parts.slice(1).join('/');
              }
              
              const command = new GetObjectCommand({
                Bucket: bucket,
                Key: key
              });
              
              const response = await s3Client.send(command);
              
              if (!response.Body) {
                console.warn(`No body in response for file: ${file}`);
                continue;
              }
              
              const chunks = [];
              for await (const chunk of response.Body as any) {
                chunks.push(chunk);
              }
              const buffer = Buffer.concat(chunks);
              
              const isCSV = file.toLowerCase().endsWith('.csv');
              
              if (isCSV) {
                await this.db.registerFileBuffer(file, new Uint8Array(buffer));
                
                await this.conn.query(`
                  INSERT INTO temp_iceberg_data
                  SELECT * FROM read_csv_auto('${file}');
                `);
              } else {
                // For Parquet files
                await this.db.registerFileBuffer(file, new Uint8Array(buffer));
                
                await this.conn.query(`
                  INSERT INTO temp_iceberg_data
                  SELECT * FROM parquet_scan('${file}');
                `);
              }
              
              console.log(`Successfully processed file: ${file}`);
            } catch (fileError) {
              console.error(`Error processing file ${file}:`, fileError);
            }
          }
          
          await this.conn.query(`
            CREATE VIEW iceberg_data AS
            SELECT * FROM temp_iceberg_data;
          `);
          
          console.log('View created successfully');
          return true;
        } else {
          console.warn('No data files found in Iceberg metadata');
        }
      } else {
        console.log('No updates to Iceberg metadata');
      }
      
      return false;
    } catch (error) {
      console.error('Error refreshing data files:', error);
      throw error;
    }
  }

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
    this.initializationPromise = null;
  }
}

const duckDBService = new DuckDBService();

export default duckDBService;
