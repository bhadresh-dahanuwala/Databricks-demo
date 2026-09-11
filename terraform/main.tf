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
  schemas = ["raw", "staging", "intermediate", "analytics", "quarantine"]
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

# 11. Schema Grants
# Grant SELECT to account users so they can query tables created by the Service Principal / Pipeline
resource "databricks_grants" "schema_grants" {
  for_each = toset(local.schemas)
  schema   = "${databricks_catalog.ecomm.name}.${databricks_schema.ecomm_schemas[each.key].name}"

  grant {
    principal  = "account users"
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

# 12. Azure Key Vault Reference for Secure Secrets
data "azurerm_key_vault" "kv" {
  name                = "kv-dbw-ecommerce"
  resource_group_name = azurerm_resource_group.dbx.name
}

data "azurerm_key_vault_secret" "supabase_password" {
  name         = "supabase-db-password"
  key_vault_id = data.azurerm_key_vault.kv.id
}

# 13. Unity Catalog Connection for Supabase PostgreSQL (Lakehouse Federation)
resource "databricks_connection" "supabase_postgres" {
  name            = "supabase_postgres"
  connection_type = "POSTGRESQL"
  comment         = "Supabase PostgreSQL connection for Lakehouse Federation"

  options = {
    host     = "aws-0-us-east-2.pooler.supabase.com"
    port     = "5432"
    user     = "postgres.upytyqlvqhkfrdswmwuj"
    password = data.azurerm_key_vault_secret.supabase_password.value
  }
}


resource "databricks_grants" "supabase_connection_grants" {
  foreign_connection = databricks_connection.supabase_postgres.name

  grant {
    principal  = "account users"
    privileges = ["USE_CONNECTION", "CREATE_FOREIGN_CATALOG"]
  }
}

# 14. Lakehouse Federation Foreign Catalog for Supabase PostgreSQL
resource "databricks_catalog" "supabase" {
  name            = "supabase"
  connection_name = databricks_connection.supabase_postgres.name
  comment         = "Lakehouse Federation Foreign Catalog for Supabase PostgreSQL"

  options = {
    database = "postgres"
  }
}

resource "databricks_grants" "supabase_catalog_grants" {
  catalog = databricks_catalog.supabase.name

  grant {
    principal  = "account users"
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "BROWSE"]
  }
}


# 15. Confluent Kafka

data "azurerm_key_vault_secret" "kafka_bootstrap" {
  name         = "confluent-kafka-bootstrap-servers"
  key_vault_id = data.azurerm_key_vault.kv.id
}

data "azurerm_key_vault_secret" "kafka_key" {
  name         = "confluent-kafka-api-key"
  key_vault_id = data.azurerm_key_vault.kv.id
}

data "azurerm_key_vault_secret" "kafka_secret" {
  name         = "confluent-kafka-api-secret"
  key_vault_id = data.azurerm_key_vault.kv.id
}

# 16. Databricks Secret Scope for Confluent Kafka
resource "databricks_secret_scope" "confluent" {
  name = "confluent"
}

resource "databricks_secret" "kafka_bootstrap" {
  key          = "bootstrap-servers"
  string_value = data.azurerm_key_vault_secret.kafka_bootstrap.value
  scope        = databricks_secret_scope.confluent.name
}

resource "databricks_secret" "kafka_key" {
  key          = "api-key"
  string_value = data.azurerm_key_vault_secret.kafka_key.value
  scope        = databricks_secret_scope.confluent.name
}

resource "databricks_secret" "kafka_secret" {
  key          = "api-secret"
  string_value = data.azurerm_key_vault_secret.kafka_secret.value
  scope        = databricks_secret_scope.confluent.name
}

resource "databricks_secret_acl" "confluent_read" {
  principal  = "users"
  permission = "READ"
  scope      = databricks_secret_scope.confluent.name
}
