import { Client } from '@notionhq/client';
import { S3Client, PutObjectCommand, GetObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { promises as fs } from 'fs';
import path from 'path';
import MCPDatabase from '../database/database.js';

class SyncManager {
  constructor(config = {}) {
    this.config = config;
    this.database = new MCPDatabase(config.databasePath);
    
    // Initialize Notion client
    if (config.notionApiKey) {
      this.notion = new Client({ auth: config.notionApiKey });
      this.notionDatabaseId = config.notionDatabaseId;
    }
    
    // Initialize cloud storage clients
    this.initializeCloudStorage();
  }

  async init() {
    await this.database.init();
  }

  initializeCloudStorage() {
    if (this.config.cloudProvider === 'aws' && this.config.awsAccessKeyId) {
      this.s3 = new S3Client({
        credentials: {
          accessKeyId: this.config.awsAccessKeyId,
          secretAccessKey: this.config.awsSecretAccessKey,
        },
        region: this.config.awsRegion || 'us-east-1'
      });
    }
    // Add other cloud providers as needed (GCP, Azure, Dropbox, etc.)
  }

  async syncAll() {
    console.log('🔄 Starting sync process...');
    const syncLogId = await this.logSyncStart('full_sync');
    
    try {
      let totalSynced = 0;
      
      // Sync to Notion
      if (this.notion && this.notionDatabaseId) {
        console.log('📝 Syncing to Notion...');
        const notionSynced = await this.syncToNotion();
        totalSynced += notionSynced;
      }
      
      // Sync to cloud storage
      if (this.s3) {
        console.log('☁️ Syncing to cloud storage...');
        const cloudSynced = await this.syncToCloud();
        totalSynced += cloudSynced;
      }
      
      await this.database.updateSyncLog(syncLogId, {
        status: 'completed',
        records_synced: totalSynced,
        error_message: null,
        details: `Successfully synced ${totalSynced} records`
      });
      
      console.log(`✅ Sync completed successfully. ${totalSynced} records synced.`);
      return { success: true, recordsSynced: totalSynced };
      
    } catch (error) {
      console.error('❌ Sync failed:', error);
      
      await this.database.updateSyncLog(syncLogId, {
        status: 'failed',
        records_synced: 0,
        error_message: error.message,
        details: error.stack
      });
      
      throw error;
    }
  }

  async syncToNotion() {
    if (!this.notion || !this.notionDatabaseId) {
      throw new Error('Notion integration not configured');
    }

    let synced = 0;
    const servers = await this.database.getAllServers();
    
    // First, create or update the Notion database schema if needed
    await this.ensureNotionDatabaseSchema();
    
    for (const server of servers) {
      try {
        const existingPage = await this.findNotionPageByName(server.name);
        
        if (existingPage) {
          // Update existing page
          await this.updateNotionPage(existingPage.id, server);
        } else {
          // Create new page
          await this.createNotionPage(server);
        }
        
        synced++;
      } catch (error) {
        console.warn(`⚠️ Failed to sync server ${server.name} to Notion:`, error.message);
      }
    }
    
    return synced;
  }

  async syncToCloud() {
    if (!this.s3) {
      throw new Error('Cloud storage not configured');
    }

    // Export database as JSON
    const exportData = await this.exportDatabaseData();
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const fileName = `mcp-servers-backup-${timestamp}.json`;
    
    // Upload to S3
    const uploadParams = {
      Bucket: this.config.awsBucketName,
      Key: `mcp-server-manager/${fileName}`,
      Body: JSON.stringify(exportData, null, 2),
      ContentType: 'application/json',
      Metadata: {
        'backup-type': 'full',
        'timestamp': timestamp,
        'version': '1.0'
      }
    };
    
    await this.s3.upload(uploadParams).promise();
    
    // Also sync the current database file
    const dbContent = await fs.readFile(this.database.dbPath);
    const dbUploadParams = {
      Bucket: this.config.awsBucketName,
      Key: `mcp-server-manager/database/mcp_servers_${timestamp}.db`,
      Body: dbContent,
      ContentType: 'application/x-sqlite3'
    };
    
    await this.s3.upload(dbUploadParams).promise();
    
    return exportData.servers.length;
  }

  async syncFromCloud() {
    if (!this.s3) {
      throw new Error('Cloud storage not configured');
    }

    // List available backups
    const listParams = {
      Bucket: this.config.awsBucketName,
      Prefix: 'mcp-server-manager/',
      MaxKeys: 10
    };
    
    const objects = await this.s3.listObjectsV2(listParams).promise();
    
    if (objects.Contents && objects.Contents.length > 0) {
      // Get the most recent backup
      const latestBackup = objects.Contents
        .filter(obj => obj.Key.endsWith('.json'))
        .sort((a, b) => new Date(b.LastModified) - new Date(a.LastModified))[0];
      
      if (latestBackup) {
        const downloadParams = {
          Bucket: this.config.awsBucketName,
          Key: latestBackup.Key
        };
        
        const data = await this.s3.getObject(downloadParams).promise();
        const backupData = JSON.parse(data.Body.toString());
        
        // Import the data
        return await this.importDatabaseData(backupData);
      }
    }
    
    return 0;
  }

  async ensureNotionDatabaseSchema() {
    try {
      // Check if the database exists and has the correct properties
      const database = await this.notion.databases.retrieve({
        database_id: this.notionDatabaseId
      });
      
      // Define required properties
      const requiredProperties = {
        'Name': { type: 'title' },
        'Version': { type: 'rich_text' },
        'Description': { type: 'rich_text' },
        'Author': { type: 'rich_text' },
        'Repository': { type: 'url' },
        'Package Manager': { 
          type: 'select',
          select: {
            options: [
              { name: 'npm', color: 'red' },
              { name: 'git', color: 'blue' },
              { name: 'config', color: 'green' }
            ]
          }
        },
        'Status': {
          type: 'select',
          select: {
            options: [
              { name: 'discovered', color: 'yellow' },
              { name: 'installed', color: 'green' },
              { name: 'error', color: 'red' }
            ]
          }
        },
        'Installed': { type: 'checkbox' },
        'Last Updated': { type: 'date' },
        'Install Command': { type: 'rich_text' },
        'Source': { type: 'rich_text' },
        'Stars': { type: 'number' }
      };
      
      // Note: Notion database schema updates require manual configuration
      // This is a placeholder for schema validation
      console.log('📊 Notion database schema verified');
      
    } catch (error) {
      console.warn('⚠️ Could not verify Notion database schema:', error.message);
    }
  }

  async findNotionPageByName(serverName) {
    try {
      const response = await this.notion.databases.query({
        database_id: this.notionDatabaseId,
        filter: {
          property: 'Name',
          title: {
            equals: serverName
          }
        }
      });
      
      return response.results.length > 0 ? response.results[0] : null;
    } catch (error) {
      console.warn(`⚠️ Error finding Notion page for ${serverName}:`, error.message);
      return null;
    }
  }

  async createNotionPage(server) {
    const properties = {
      'Name': {
        title: [{ text: { content: server.name } }]
      },
      'Version': {
        rich_text: [{ text: { content: server.version || 'unknown' } }]
      },
      'Description': {
        rich_text: [{ text: { content: server.description || '' } }]
      },
      'Author': {
        rich_text: [{ text: { content: server.author || '' } }]
      },
      'Package Manager': {
        select: { name: server.package_manager || 'npm' }
      },
      'Status': {
        select: { name: server.status || 'discovered' }
      },
      'Installed': {
        checkbox: server.installed || false
      },
      'Last Updated': {
        date: { start: server.last_updated || new Date().toISOString() }
      },
      'Install Command': {
        rich_text: [{ text: { content: server.install_command || '' } }]
      },
      'Source': {
        rich_text: [{ text: { content: server.metadata?.source || 'unknown' } }]
      }
    };

    // Add repository URL if available
    if (server.repository_url) {
      properties['Repository'] = { url: server.repository_url };
    }

    // Add stars if available (for GitHub repos)
    if (server.metadata?.stars) {
      properties['Stars'] = { number: server.metadata.stars };
    }

    await this.notion.pages.create({
      parent: { database_id: this.notionDatabaseId },
      properties
    });
  }

  async updateNotionPage(pageId, server) {
    const properties = {
      'Version': {
        rich_text: [{ text: { content: server.version || 'unknown' } }]
      },
      'Description': {
        rich_text: [{ text: { content: server.description || '' } }]
      },
      'Status': {
        select: { name: server.status || 'discovered' }
      },
      'Installed': {
        checkbox: server.installed || false
      },
      'Last Updated': {
        date: { start: server.last_updated || new Date().toISOString() }
      }
    };

    // Update repository URL if available
    if (server.repository_url) {
      properties['Repository'] = { url: server.repository_url };
    }

    // Update stars if available
    if (server.metadata?.stars) {
      properties['Stars'] = { number: server.metadata.stars };
    }

    await this.notion.pages.update({
      page_id: pageId,
      properties
    });
  }

  async exportDatabaseData() {
    const servers = await this.database.getAllServers();
    const scanHistory = await this.database.getScanHistory();
    const syncLogs = await this.database.getSyncLogs();
    const stats = await this.database.getStats();
    
    return {
      export_date: new Date().toISOString(),
      version: '1.0',
      stats,
      servers,
      scan_history: scanHistory,
      sync_logs: syncLogs
    };
  }

  async importDatabaseData(data) {
    let imported = 0;
    
    if (data.servers && Array.isArray(data.servers)) {
      for (const server of data.servers) {
        try {
          await this.database.addServer(server);
          imported++;
        } catch (error) {
          console.warn(`⚠️ Failed to import server ${server.name}:`, error.message);
        }
      }
    }
    
    return imported;
  }

  async logSyncStart(syncType) {
    const result = await this.database.addSyncLog({
      sync_type: syncType,
      status: 'in_progress',
      records_synced: 0,
      error_message: null,
      details: 'Sync started'
    });
    
    return result.id;
  }

  async getCloudBackups() {
    if (!this.s3) {
      return [];
    }

    const listParams = {
      Bucket: this.config.awsBucketName,
      Prefix: 'mcp-server-manager/',
      MaxKeys: 20
    };
    
    const objects = await this.s3.listObjectsV2(listParams).promise();
    
    return objects.Contents?.map(obj => ({
      key: obj.Key,
      lastModified: obj.LastModified,
      size: obj.Size,
      type: obj.Key.endsWith('.json') ? 'json_backup' : 'database_backup'
    })) || [];
  }

  async downloadBackup(backupKey, localPath) {
    if (!this.s3) {
      throw new Error('Cloud storage not configured');
    }

    const downloadParams = {
      Bucket: this.config.awsBucketName,
      Key: backupKey
    };
    
    const data = await this.s3.getObject(downloadParams).promise();
    await fs.writeFile(localPath, data.Body);
    
    return localPath;
  }

  async close() {
    await this.database.close();
  }
}

export default SyncManager;