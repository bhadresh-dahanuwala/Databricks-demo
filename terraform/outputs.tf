output "key_vault_name" {
  description = "Name of the Azure Key Vault for secrets"
  value       = data.azurerm_key_vault.kv.name
}

output "key_vault_uri" {
  description = "URI of the Azure Key Vault"
  value       = data.azurerm_key_vault.kv.vault_uri
}

output "supabase_connection_name" {
  description = "Name of the Unity Catalog PostgreSQL connection for Supabase"
  value       = databricks_connection.supabase_postgres.name
}
