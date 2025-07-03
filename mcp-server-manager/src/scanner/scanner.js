import { promises as fs } from 'fs';
import path from 'path';
import { glob } from 'glob';
import { exec } from 'child_process';
import { promisify } from 'util';
import axios from 'axios';
import yaml from 'yaml';
import MCPDatabase from '../database/database.js';

const execAsync = promisify(exec);

class MCPScanner {
  constructor(config = {}) {
    this.config = {
      discoveryPaths: config.discoveryPaths || [
        '/usr/local/lib/node_modules',
        '~/.config/mcp',
        './mcp-servers',
        '~/.npm-global/lib/node_modules'
      ],
      githubSearchEnabled: config.githubSearchEnabled || true,
      githubToken: config.githubToken || null,
      ...config
    };
    this.database = new MCPDatabase(config.databasePath);
  }

  async init() {
    await this.database.init();
  }

  async scan() {
    const startTime = Date.now();
    let totalFound = 0;
    let newServers = 0;
    let updatedServers = 0;
    
    console.log('🔍 Starting MCP server discovery scan...');

    try {
      // Scan local file system
      console.log('📁 Scanning local file system...');
      const localResults = await this.scanLocalFilesystem();
      totalFound += localResults.found;
      newServers += localResults.new;
      updatedServers += localResults.updated;

      // Scan npm packages
      console.log('📦 Scanning npm packages...');
      const npmResults = await this.scanNpmPackages();
      totalFound += npmResults.found;
      newServers += npmResults.new;
      updatedServers += npmResults.updated;

      // Scan GitHub repositories
      if (this.config.githubSearchEnabled) {
        console.log('🐙 Scanning GitHub repositories...');
        const githubResults = await this.scanGitHubRepositories();
        totalFound += githubResults.found;
        newServers += githubResults.new;
        updatedServers += githubResults.updated;
      }

      // Scan system installations
      console.log('⚙️ Scanning system installations...');
      const systemResults = await this.scanSystemInstallations();
      totalFound += systemResults.found;
      newServers += systemResults.new;
      updatedServers += systemResults.updated;

      const duration = Date.now() - startTime;
      
      // Log scan results
      await this.database.addScanHistory({
        scan_type: 'full_scan',
        servers_found: totalFound,
        new_servers: newServers,
        updated_servers: updatedServers,
        scan_duration: duration,
        status: 'completed',
        details: JSON.stringify({
          local: localResults,
          npm: npmResults,
          github: githubResults,
          system: systemResults
        })
      });

      console.log(`✅ Scan completed in ${duration}ms`);
      console.log(`📊 Results: ${totalFound} servers found, ${newServers} new, ${updatedServers} updated`);

      return {
        totalFound,
        newServers,
        updatedServers,
        duration
      };

    } catch (error) {
      console.error('❌ Scan failed:', error);
      
      await this.database.addScanHistory({
        scan_type: 'full_scan',
        servers_found: 0,
        new_servers: 0,
        updated_servers: 0,
        scan_duration: Date.now() - startTime,
        status: 'failed',
        details: error.message
      });

      throw error;
    }
  }

  async scanLocalFilesystem() {
    let found = 0;
    let newCount = 0;
    let updated = 0;

    for (const searchPath of this.config.discoveryPaths) {
      try {
        const expandedPath = searchPath.replace('~', process.env.HOME || process.env.USERPROFILE || '');
        
        // Look for package.json files that might be MCP servers
        const packageFiles = await glob(`${expandedPath}/**/package.json`, {
          ignore: ['**/node_modules/**', '**/.*/**'],
          maxDepth: 5
        });

        for (const packageFile of packageFiles) {
          try {
            const packageData = JSON.parse(await fs.readFile(packageFile, 'utf8'));
            
            if (this.isMCPServer(packageData)) {
              const serverData = await this.extractServerData(packageData, packageFile, 'local');
              const existing = await this.database.getServer(serverData.name);
              
              if (!existing) {
                await this.database.addServer(serverData);
                newCount++;
              } else if (this.hasServerChanged(existing, serverData)) {
                await this.database.addServer(serverData);
                updated++;
              }
              
              found++;
            }
          } catch (err) {
            console.warn(`⚠️ Could not parse ${packageFile}:`, err.message);
          }
        }

        // Look for MCP configuration files
        const mcpConfigFiles = await glob(`${expandedPath}/**/*.mcp.{json,yaml,yml}`, {
          ignore: ['**/node_modules/**'],
          maxDepth: 3
        });

        for (const configFile of mcpConfigFiles) {
          try {
            const serverData = await this.parseMCPConfig(configFile);
            if (serverData) {
              const existing = await this.database.getServer(serverData.name);
              
              if (!existing) {
                await this.database.addServer(serverData);
                newCount++;
              } else if (this.hasServerChanged(existing, serverData)) {
                await this.database.addServer(serverData);
                updated++;
              }
              
              found++;
            }
          } catch (err) {
            console.warn(`⚠️ Could not parse MCP config ${configFile}:`, err.message);
          }
        }

      } catch (err) {
        console.warn(`⚠️ Could not scan path ${searchPath}:`, err.message);
      }
    }

    return { found, new: newCount, updated };
  }

  async scanNpmPackages() {
    let found = 0;
    let newCount = 0;
    let updated = 0;

    try {
      // Search for MCP-related packages using npm search
      const searchTerms = [
        'mcp-server',
        'model-context-protocol',
        '@modelcontextprotocol',
        'mcp server'
      ];

      for (const term of searchTerms) {
        try {
          const { stdout } = await execAsync(`npm search ${term} --json --long`, {
            timeout: 30000
          });
          
          const packages = JSON.parse(stdout);
          
          for (const pkg of packages.slice(0, 50)) { // Limit results
            if (this.isLikelyMCPPackage(pkg)) {
              const serverData = await this.extractNpmServerData(pkg);
              const existing = await this.database.getServer(serverData.name);
              
              if (!existing) {
                await this.database.addServer(serverData);
                newCount++;
              } else if (this.hasServerChanged(existing, serverData)) {
                await this.database.addServer(serverData);
                updated++;
              }
              
              found++;
            }
          }
        } catch (err) {
          console.warn(`⚠️ NPM search for "${term}" failed:`, err.message);
        }
      }
    } catch (err) {
      console.warn('⚠️ NPM package scanning failed:', err.message);
    }

    return { found, new: newCount, updated };
  }

  async scanGitHubRepositories() {
    let found = 0;
    let newCount = 0;
    let updated = 0;

    const searchQueries = [
      'mcp-server in:name',
      'model-context-protocol in:readme',
      '"mcp server" in:readme',
      '@modelcontextprotocol in:name'
    ];

    const headers = {
      'Accept': 'application/vnd.github.v3+json',
      'User-Agent': 'MCP-Server-Manager'
    };

    if (this.config.githubToken) {
      headers['Authorization'] = `token ${this.config.githubToken}`;
    }

    for (const query of searchQueries) {
      try {
        const response = await axios.get(`https://api.github.com/search/repositories`, {
          params: {
            q: query,
            sort: 'updated',
            per_page: 30
          },
          headers
        });

        for (const repo of response.data.items) {
          try {
            const serverData = await this.extractGitHubServerData(repo);
            if (serverData) {
              const existing = await this.database.getServer(serverData.name);
              
              if (!existing) {
                await this.database.addServer(serverData);
                newCount++;
              } else if (this.hasServerChanged(existing, serverData)) {
                await this.database.addServer(serverData);
                updated++;
              }
              
              found++;
            }
          } catch (err) {
            console.warn(`⚠️ Could not process GitHub repo ${repo.name}:`, err.message);
          }
        }

        // Respect GitHub API rate limiting
        await new Promise(resolve => setTimeout(resolve, 1000));
        
      } catch (err) {
        console.warn(`⚠️ GitHub search for "${query}" failed:`, err.message);
        if (err.response?.status === 403) {
          console.warn('⚠️ GitHub API rate limit reached, skipping remaining searches');
          break;
        }
      }
    }

    return { found, new: newCount, updated };
  }

  async scanSystemInstallations() {
    let found = 0;
    let newCount = 0;
    let updated = 0;

    // Check for globally installed packages
    try {
      const { stdout: globalPackages } = await execAsync('npm list -g --depth=0 --json', {
        timeout: 15000
      });
      
      const packageData = JSON.parse(globalPackages);
      
      if (packageData.dependencies) {
        for (const [name, info] of Object.entries(packageData.dependencies)) {
          if (name.includes('mcp') || name.includes('model-context-protocol')) {
            try {
              const packagePath = path.join(info.path || '', 'package.json');
              const packageInfo = JSON.parse(await fs.readFile(packagePath, 'utf8'));
              
              if (this.isMCPServer(packageInfo)) {
                const serverData = await this.extractServerData(packageInfo, packagePath, 'global');
                const existing = await this.database.getServer(serverData.name);
                
                if (!existing) {
                  await this.database.addServer({
                    ...serverData,
                    installed: true,
                    status: 'installed'
                  });
                  newCount++;
                } else {
                  await this.database.updateServerStatus(serverData.name, 'installed', true);
                  updated++;
                }
                
                found++;
              }
            } catch (err) {
              console.warn(`⚠️ Could not check global package ${name}:`, err.message);
            }
          }
        }
      }
    } catch (err) {
      console.warn('⚠️ Could not scan global npm packages:', err.message);
    }

    return { found, new: newCount, updated };
  }

  isMCPServer(packageData) {
    if (!packageData) return false;

    // Check package name
    if (packageData.name && packageData.name.toLowerCase().includes('mcp')) {
      return true;
    }

    // Check keywords
    if (packageData.keywords && Array.isArray(packageData.keywords)) {
      const mcpKeywords = ['mcp', 'model-context-protocol', 'mcp-server'];
      if (packageData.keywords.some(k => mcpKeywords.includes(k.toLowerCase()))) {
        return true;
      }
    }

    // Check description
    if (packageData.description && packageData.description.toLowerCase().includes('mcp')) {
      return true;
    }

    // Check for MCP-specific dependencies
    const allDeps = {
      ...packageData.dependencies,
      ...packageData.devDependencies,
      ...packageData.peerDependencies
    };
    
    if (allDeps) {
      const mcpDeps = Object.keys(allDeps).filter(dep => 
        dep.includes('@modelcontextprotocol') || 
        dep.includes('mcp-') ||
        dep === 'mcp'
      );
      
      if (mcpDeps.length > 0) {
        return true;
      }
    }

    return false;
  }

  isLikelyMCPPackage(npmPackage) {
    return this.isMCPServer({
      name: npmPackage.name,
      description: npmPackage.description,
      keywords: npmPackage.keywords
    });
  }

  async extractServerData(packageData, packagePath, source) {
    return {
      name: packageData.name,
      version: packageData.version || 'unknown',
      description: packageData.description || '',
      author: this.extractAuthor(packageData.author),
      repository_url: this.extractRepositoryUrl(packageData.repository),
      package_manager: 'npm',
      install_command: `npm install ${packageData.name}`,
      config_path: packagePath,
      status: source === 'global' ? 'installed' : 'discovered',
      installed: source === 'global',
      metadata: {
        source,
        keywords: packageData.keywords || [],
        homepage: packageData.homepage,
        license: packageData.license,
        engines: packageData.engines,
        bin: packageData.bin,
        scripts: packageData.scripts
      }
    };
  }

  async extractNpmServerData(npmPackage) {
    return {
      name: npmPackage.name,
      version: npmPackage.version || 'unknown',
      description: npmPackage.description || '',
      author: npmPackage.publisher?.username || npmPackage.author || '',
      repository_url: '',
      package_manager: 'npm',
      install_command: `npm install ${npmPackage.name}`,
      config_path: '',
      status: 'discovered',
      installed: false,
      metadata: {
        source: 'npm_search',
        keywords: npmPackage.keywords || [],
        date: npmPackage.date,
        npmScore: npmPackage.searchScore
      }
    };
  }

  async extractGitHubServerData(repo) {
    // Try to fetch package.json to get more details
    try {
      const packageResponse = await axios.get(`https://api.github.com/repos/${repo.full_name}/contents/package.json`, {
        headers: {
          'Accept': 'application/vnd.github.v3+json',
          'User-Agent': 'MCP-Server-Manager'
        }
      });

      if (packageResponse.data.content) {
        const packageContent = Buffer.from(packageResponse.data.content, 'base64').toString();
        const packageData = JSON.parse(packageContent);
        
        if (this.isMCPServer(packageData)) {
          return {
            name: packageData.name || repo.name,
            version: packageData.version || 'unknown',
            description: packageData.description || repo.description || '',
            author: this.extractAuthor(packageData.author) || repo.owner.login,
            repository_url: repo.html_url,
            package_manager: 'git',
            install_command: `git clone ${repo.clone_url}`,
            config_path: '',
            status: 'discovered',
            installed: false,
            metadata: {
              source: 'github',
              stars: repo.stargazers_count,
              forks: repo.forks_count,
              updated_at: repo.updated_at,
              language: repo.language,
              topics: repo.topics || []
            }
          };
        }
      }
    } catch (err) {
      // If we can't fetch package.json, create a basic entry
      if (repo.name.toLowerCase().includes('mcp') || 
          (repo.description && repo.description.toLowerCase().includes('mcp'))) {
        return {
          name: repo.name,
          version: 'unknown',
          description: repo.description || '',
          author: repo.owner.login,
          repository_url: repo.html_url,
          package_manager: 'git',
          install_command: `git clone ${repo.clone_url}`,
          config_path: '',
          status: 'discovered',
          installed: false,
          metadata: {
            source: 'github',
            stars: repo.stargazers_count,
            forks: repo.forks_count,
            updated_at: repo.updated_at,
            language: repo.language,
            topics: repo.topics || []
          }
        };
      }
    }

    return null;
  }

  async parseMCPConfig(configFile) {
    const content = await fs.readFile(configFile, 'utf8');
    const ext = path.extname(configFile);
    
    let config;
    if (ext === '.json') {
      config = JSON.parse(content);
    } else if (ext === '.yaml' || ext === '.yml') {
      config = yaml.parse(content);
    } else {
      return null;
    }

    if (config && config.server) {
      return {
        name: config.server.name || path.basename(configFile, ext),
        version: config.server.version || 'unknown',
        description: config.server.description || '',
        author: config.server.author || '',
        repository_url: config.server.repository || '',
        package_manager: 'config',
        install_command: config.server.install || '',
        config_path: configFile,
        status: 'discovered',
        installed: false,
        metadata: {
          source: 'mcp_config',
          config: config
        }
      };
    }

    return null;
  }

  extractAuthor(author) {
    if (typeof author === 'string') {
      return author;
    }
    if (typeof author === 'object' && author.name) {
      return author.name;
    }
    return '';
  }

  extractRepositoryUrl(repository) {
    if (typeof repository === 'string') {
      return repository;
    }
    if (typeof repository === 'object' && repository.url) {
      return repository.url;
    }
    return '';
  }

  hasServerChanged(existing, newData) {
    return existing.version !== newData.version ||
           existing.description !== newData.description ||
           existing.repository_url !== newData.repository_url;
  }

  async close() {
    await this.database.close();
  }
}

export default MCPScanner;