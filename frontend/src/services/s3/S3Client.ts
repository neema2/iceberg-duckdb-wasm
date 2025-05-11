import { S3Client, GetObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

interface S3Config {
  endpoint: string;
  region: string;
  forcePathStyle: boolean;
  credentials?: {
    accessKeyId: string;
    secretAccessKey: string;
  };
}

class S3Service {
  private client: S3Client;
  private bucket: string;

  constructor(config: S3Config, bucket: string) {
    this.client = new S3Client({
      endpoint: config.endpoint,
      region: config.region,
      forcePathStyle: config.forcePathStyle,
      credentials: config.credentials,
    });
    this.bucket = bucket;
  }

  async listObjects(prefix: string): Promise<string[]> {
    try {
      const command = new ListObjectsV2Command({
        Bucket: this.bucket,
        Prefix: prefix,
      });

      const response = await this.client.send(command);
      return (response.Contents || []).map((item) => item.Key || "");
    } catch (error) {
      console.error("Error listing objects:", error);
      throw error;
    }
  }

  async getObject(key: string): Promise<ArrayBuffer> {
    try {
      const command = new GetObjectCommand({
        Bucket: this.bucket,
        Key: key,
      });

      const response = await this.client.send(command);
      if (!response.Body) {
        throw new Error("Empty response body");
      }
      
      return await response.Body.transformToByteArray();
    } catch (error) {
      console.error(`Error getting object ${key}:`, error);
      throw error;
    }
  }

  async getObjectAsJson<T>(key: string): Promise<T> {
    try {
      const arrayBuffer = await this.getObject(key);
      const decoder = new TextDecoder("utf-8");
      const jsonString = decoder.decode(arrayBuffer);
      return JSON.parse(jsonString) as T;
    } catch (error) {
      console.error(`Error parsing object ${key} as JSON:`, error);
      throw error;
    }
  }

  async getSignedUrl(key: string, expiresIn = 3600): Promise<string> {
    try {
      const command = new GetObjectCommand({
        Bucket: this.bucket,
        Key: key,
      });

      return await getSignedUrl(this.client, command, { expiresIn });
    } catch (error) {
      console.error(`Error generating signed URL for ${key}:`, error);
      throw error;
    }
  }
}

const defaultConfig: S3Config = {
  endpoint: "http://localhost:9000",
  region: "us-east-1",
  forcePathStyle: true,
  credentials: {
    accessKeyId: 'minioadmin',
    secretAccessKey: 'minioadmin'
  }
};

const s3Client = new S3Service(defaultConfig, "iceberg-data");

export { S3Service, type S3Config, s3Client };
export default s3Client;
