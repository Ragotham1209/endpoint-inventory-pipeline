// ==================================================================================
// Endpoint Inventory Pipeline — Production Infrastructure Template 
// Azure Bicep IaC — Zero-Click Cloud Provisioning
// ==================================================================================

@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Prefix for all resources (e.g., invprod)')
param resourcePrefix string = 'invprod'

// ==================================================================================
// 1. Virtual Network (Corporate Network Isolation)
// ==================================================================================
resource vnet 'Microsoft.Network/virtualNetworks@2023-09-01' = {
  name: '${resourcePrefix}-vnet'
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: ['10.0.0.0/16']
    }
    subnets: [
      {
        name: 'sn-functions'
        properties: {
          addressPrefix: '10.0.1.0/24'
          delegations: [
            {
              name: 'delegation'
              properties: {
                serviceName: 'Microsoft.Web/serverFarms'
              }
            }
          ]
        }
      }
      {
        name: 'sn-databricks-public'
        properties: {
          addressPrefix: '10.0.2.0/24'
          delegations: [
            {
              name: 'delegation'
              properties: {
                serviceName: 'Microsoft.Databricks/workspaces'
              }
            }
          ]
          networkSecurityGroup: {
            id: nsgDatabricks.id
          }
        }
      }
      {
        name: 'sn-databricks-private'
        properties: {
          addressPrefix: '10.0.3.0/24'
          delegations: [
            {
              name: 'delegation'
              properties: {
                serviceName: 'Microsoft.Databricks/workspaces'
              }
            }
          ]
          networkSecurityGroup: {
            id: nsgDatabricks.id
          }
        }
      }
    ]
  }
}

// ==================================================================================
// 2. Network Security Group for Databricks
// ==================================================================================
resource nsgDatabricks 'Microsoft.Network/networkSecurityGroups@2023-09-01' = {
  name: '${resourcePrefix}-nsg-dbw'
  location: location
}

// ==================================================================================
// 3. ADLS Gen2 Storage Account (Zone-Redundant)
// ==================================================================================
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: '${resourcePrefix}lake'
  location: location
  sku: {
    name: 'Standard_ZRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    accessTier: 'Hot'
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Allow'
    }
  }
}

// Data Lake Containers (Medallion Architecture)
resource rawContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageAccount.name}/default/raw'
}

resource silverContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageAccount.name}/default/silver'
}

resource goldContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageAccount.name}/default/gold'
}

resource quarantineContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageAccount.name}/default/quarantine'
}

// ==================================================================================
// 4. Azure Function App (Elastic Premium EP1)
// ==================================================================================
resource appServicePlan 'Microsoft.Web/serverfarms@2023-01-01' = {
  name: '${resourcePrefix}-asp'
  location: location
  kind: 'elastic'
  sku: {
    name: 'EP1'
    tier: 'ElasticPremium'
  }
  properties: {
    reserved: true  // Linux
  }
}

resource functionApp 'Microsoft.Web/sites@2023-01-01' = {
  name: '${resourcePrefix}-func'
  location: location
  kind: 'functionapp,linux'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: appServicePlan.id
    httpsOnly: true
    siteConfig: {
      pythonVersion: '3.11'
      linuxFxVersion: 'Python|3.11'
      appSettings: [
        { name: 'AzureWebJobsStorage'; value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};EndpointSuffix=core.windows.net;AccountKey=${storageAccount.listKeys().keys[0].value}' }
        { name: 'FUNCTIONS_EXTENSION_VERSION'; value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME'; value: 'python' }
        { name: 'ADLS_ACCOUNT_NAME'; value: storageAccount.name }
      ]
    }
    virtualNetworkSubnetId: vnet.properties.subnets[0].id
  }
}

// ==================================================================================
// 5. Azure Databricks Workspace (Premium — Unity Catalog Enabled)
// ==================================================================================
resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: '${resourcePrefix}-dbw'
  location: location
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', '${resourcePrefix}-dbw-managed')
    parameters: {
      customVirtualNetworkId: {
        value: vnet.id
      }
      customPublicSubnetName: {
        value: 'sn-databricks-public'
      }
      customPrivateSubnetName: {
        value: 'sn-databricks-private'
      }
      enableNoPublicIp: {
        value: true
      }
    }
  }
}

// ==================================================================================
// 6. Key Vault (Secrets Management)
// ==================================================================================
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: '${resourcePrefix}-kv'
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    enableSoftDelete: true
  }
}

// ==================================================================================
// 7. Application Insights (Monitoring)
// ==================================================================================
resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: '${resourcePrefix}-insights'
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    RetentionInDays: 30
  }
}

// ==================================================================================
// Outputs
// ==================================================================================
output functionAppName string = functionApp.name
output storageAccountName string = storageAccount.name
output databricksWorkspaceName string = databricksWorkspace.name
output keyVaultName string = keyVault.name
