import sqlite3 from 'sqlite3';
import { promises as fs } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

class MCPDatabase {
  constructor(dbPath = './data/mcp_servers.db') {
    this.dbPath = dbPath;
    this.db = null;
  }

  async init() {
    // Ensure data directory exists
    const dataDir = path.dirname(this.dbPath);
    await fs.mkdir(dataDir, { recursive: true });

    return new Promise((resolve, reject) => {
      this.db = new sqlite3.Database(this.dbPath, (err) => {
        if (err) {
          reject(err);
        } else {
          this.createTables().then(resolve).catch(reject);
        }
      });
    });
  }

  async createTables() {
    const tables = [
      `CREATE TABLE IF NOT EXISTS mcp_servers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        version TEXT,
        description TEXT,
        author TEXT,
        repository_url TEXT,
        package_manager TEXT,
        install_command TEXT,
        config_path TEXT,
        status TEXT DEFAULT 'discovered',
        installed BOOLEAN DEFAULT FALSE,
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        metadata TEXT
      )`,
      
      `CREATE TABLE IF NOT EXISTS scan_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_type TEXT NOT NULL,
        scan_date DATETIME DEFAULT CURRENT_TIMESTAMP,
        servers_found INTEGER DEFAULT 0,
        new_servers INTEGER DEFAULT 0,
        updated_servers INTEGER DEFAULT 0,
        scan_duration INTEGER,
        status TEXT DEFAULT 'completed',
        details TEXT
      )`,
      
      `CREATE TABLE IF NOT EXISTS sync_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sync_type TEXT NOT NULL,
        sync_date DATETIME DEFAULT CURRENT_TIMESTAMP,
        status TEXT DEFAULT 'pending',
        records_synced INTEGER DEFAULT 0,
        error_message TEXT,
        details TEXT
      )`
    ];

    for (const table of tables) {
      await this.run(table);
    }
  }

  run(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.db.run(sql, params, function(err) {
        if (err) {
          reject(err);
        } else {
          resolve({ id: this.lastID, changes: this.changes });
        }
      });
    });
  }

  get(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.db.get(sql, params, (err, row) => {
        if (err) {
          reject(err);
        } else {
          resolve(row);
        }
      });
    });
  }

  all(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.db.all(sql, params, (err, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      });
    });
  }

  // MCP Server operations
  async addServer(serverData) {
    const {
      name,
      version,
      description,
      author,
      repository_url,
      package_manager,
      install_command,
      config_path,
      status = 'discovered',
      installed = false,
      metadata = '{}'
    } = serverData;

    const sql = `
      INSERT OR REPLACE INTO mcp_servers 
      (name, version, description, author, repository_url, package_manager, 
       install_command, config_path, status, installed, metadata, last_updated)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    `;

    return await this.run(sql, [
      name, version, description, author, repository_url,
      package_manager, install_command, config_path, status,
      installed, typeof metadata === 'string' ? metadata : JSON.stringify(metadata)
    ]);
  }

  async getServer(name) {
    const sql = 'SELECT * FROM mcp_servers WHERE name = ?';
    const server = await this.get(sql, [name]);
    if (server && server.metadata) {
      try {
        server.metadata = JSON.parse(server.metadata);
      } catch (e) {
        server.metadata = {};
      }
    }
    return server;
  }

  async getAllServers() {
    const sql = 'SELECT * FROM mcp_servers ORDER BY name ASC';
    const servers = await this.all(sql);
    return servers.map(server => {
      if (server.metadata) {
        try {
          server.metadata = JSON.parse(server.metadata);
        } catch (e) {
          server.metadata = {};
        }
      }
      return server;
    });
  }

  async getInstalledServers() {
    const sql = 'SELECT * FROM mcp_servers WHERE installed = TRUE ORDER BY name ASC';
    return await this.all(sql);
  }

  async updateServerStatus(name, status, installed = null) {
    const updates = ['status = ?', 'last_updated = CURRENT_TIMESTAMP'];
    const params = [status];

    if (installed !== null) {
      updates.push('installed = ?');
      params.push(installed);
    }

    params.push(name);
    
    const sql = `UPDATE mcp_servers SET ${updates.join(', ')} WHERE name = ?`;
    return await this.run(sql, params);
  }

  async deleteServer(name) {
    const sql = 'DELETE FROM mcp_servers WHERE name = ?';
    return await this.run(sql, [name]);
  }

  // Scan history operations
  async addScanHistory(scanData) {
    const {
      scan_type,
      servers_found = 0,
      new_servers = 0,
      updated_servers = 0,
      scan_duration = 0,
      status = 'completed',
      details = ''
    } = scanData;

    const sql = `
      INSERT INTO scan_history 
      (scan_type, servers_found, new_servers, updated_servers, scan_duration, status, details)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `;

    return await this.run(sql, [
      scan_type, servers_found, new_servers, updated_servers,
      scan_duration, status, details
    ]);
  }

  async getScanHistory(limit = 50) {
    const sql = 'SELECT * FROM scan_history ORDER BY scan_date DESC LIMIT ?';
    return await this.all(sql, [limit]);
  }

  // Sync log operations
  async addSyncLog(syncData) {
    const {
      sync_type,
      status = 'pending',
      records_synced = 0,
      error_message = null,
      details = ''
    } = syncData;

    const sql = `
      INSERT INTO sync_logs (sync_type, status, records_synced, error_message, details)
      VALUES (?, ?, ?, ?, ?)
    `;

    return await this.run(sql, [sync_type, status, records_synced, error_message, details]);
  }

  async updateSyncLog(id, updateData) {
    const { status, records_synced, error_message, details } = updateData;
    
    const sql = `
      UPDATE sync_logs 
      SET status = ?, records_synced = ?, error_message = ?, details = ?
      WHERE id = ?
    `;

    return await this.run(sql, [status, records_synced, error_message, details, id]);
  }

  async getSyncLogs(limit = 50) {
    const sql = 'SELECT * FROM sync_logs ORDER BY sync_date DESC LIMIT ?';
    return await this.all(sql, [limit]);
  }

  async getStats() {
    const totalServers = await this.get('SELECT COUNT(*) as count FROM mcp_servers');
    const installedServers = await this.get('SELECT COUNT(*) as count FROM mcp_servers WHERE installed = TRUE');
    const lastScan = await this.get('SELECT * FROM scan_history ORDER BY scan_date DESC LIMIT 1');
    const lastSync = await this.get('SELECT * FROM sync_logs ORDER BY sync_date DESC LIMIT 1');

    return {
      total_servers: totalServers.count,
      installed_servers: installedServers.count,
      last_scan: lastScan,
      last_sync: lastSync
    };
  }

  async close() {
    return new Promise((resolve) => {
      if (this.db) {
        this.db.close(resolve);
      } else {
        resolve();
      }
    });
  }
}

export default MCPDatabase;