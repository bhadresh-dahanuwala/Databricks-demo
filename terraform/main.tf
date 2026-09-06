data "azurerm_client_config" "current" {}

# 1. Resource Group
resource "azurerm_resource_group" "dbx" {
  name     = "rg-dbx"
  location = "East US"
}

# 2. Azure Databricks Workspace
resource "azurerm_databricks_workspace" "dbw" {
  name                = "dbw-ecommerce"
  resource_group_name = azurerm_resource_group.dbx.name
  location            = azurerm_resource_group.dbx.location
  sku                 = "premium" # Premium is required for Unity Catalog
}

# 3. Access Connector for Azure Databricks (Managed Identity)
resource "azurerm_databricks_access_connector" "ext_access_connector" {
  name                = "dbx-access-connector"
  resource_group_name = azurerm_resource_group.dbx.name
  location            = azurerm_resource_group.dbx.location
  identity {
    type = "SystemAssigned"
  }
}

# 4. Storage Account for Unity Catalog External Storage
resource "azurerm_storage_account" "ext_storage" {
  name                     = "stdbxbad"
  resource_group_name      = azurerm_resource_group.dbx.name
  location                 = azurerm_resource_group.dbx.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Must be true (ADLS Gen2) for Unity Catalog
}

resource "azurerm_storage_container" "ext_container" {
  name                  = "ecomm-ext"
  storage_account_id    = azurerm_storage_account.ext_storage.id
  container_access_type = "private"
}

# 5. Role Assignments
# Grant Access Connector permission on the Storage Account
resource "azurerm_role_assignment" "ext_storage_role" {
  scope                = azurerm_storage_account.ext_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.ext_access_connector.identity[0].principal_id
}

resource "azurerm_role_assignment" "ext_storage_account_contributor" {
  scope                = azurerm_storage_account.ext_storage.id
  role_definition_name = "Storage Account Contributor"
  principal_id         = azurerm_databricks_access_connector.ext_access_connector.identity[0].principal_id
}

resource "azurerm_role_assignment" "ext_eventgrid_contributor" {
  scope                = azurerm_storage_account.ext_storage.id
  role_definition_name = "EventGrid EventSubscription Contributor"
  principal_id         = azurerm_databricks_access_connector.ext_access_connector.identity[0].principal_id
}

resource "azurerm_role_assignment" "ext_storage_queue_contributor" {
  scope                = azurerm_storage_account.ext_storage.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = azurerm_databricks_access_connector.ext_access_connector.identity[0].principal_id
}

# Grant Access Connector 'Reader' role ON ITSELF (Databricks UC requirement for Managed Identities)
resource "azurerm_role_assignment" "ext_access_connector_reader" {
  scope                = azurerm_databricks_access_connector.ext_access_connector.id
  role_definition_name = "Reader"
  principal_id         = azurerm_databricks_access_connector.ext_access_connector.identity[0].principal_id
}

# 6. Databricks Storage Credential
resource "databricks_storage_credential" "ext_storage_cred" {
  name = "mi_cred_stdbxbad"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.ext_access_connector.id
  }

  # Ensure the role assignments exist before creating the credential
  depends_on = [
    azurerm_role_assignment.ext_storage_role,
    azurerm_role_assignment.ext_storage_account_contributor,
    azurerm_role_assignment.ext_eventgrid_contributor,
    azurerm_role_assignment.ext_storage_queue_contributor,
    azurerm_role_assignment.ext_access_connector_reader
  ]
}

# 7. Databricks External Location
resource "databricks_external_location" "ext_loc" {
  name            = "ext_loc_ecomm"
  url             = "abfss://${azurerm_storage_container.ext_container.name}@${azurerm_storage_account.ext_storage.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.ext_storage_cred.name
  force_destroy   = true
}

# 8. Databricks Catalog
resource "databricks_catalog" "ecomm" {
  name          = "ecomm"
  storage_root  = databricks_external_location.ext_loc.url
  force_destroy = true
  owner         = "account users"
}

# 9. Databricks Schemas
locals {
  schemas = ["raw", "staging", "intermediate", "analytics"]
}

resource "databricks_schema" "ecomm_schemas" {
  for_each     = toset(local.schemas)
  catalog_name = databricks_catalog.ecomm.name
  name         = each.key
  owner        = "account users"
}

# 10. Databricks Volumes
# Creating a managed volume inside each schema
resource "databricks_volume" "ecomm_volumes" {
  for_each     = toset(local.schemas)
  name         = "${each.key}_vol"
  catalog_name = databricks_catalog.ecomm.name
  schema_name  = databricks_schema.ecomm_schemas[each.key].name
  volume_type  = "MANAGED"
  owner        = "account users"
}
