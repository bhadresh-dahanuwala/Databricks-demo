terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
  
  # Configure the backend to store state in Azure Storage
  backend "azurerm" {
    # TODO: Replace these with your actual storage account details
    resource_group_name  = "<YOUR-TFSTATE-RESOURCE-GROUP>"
    storage_account_name = "<YOUR-TFSTATE-STORAGE-ACCOUNT>"
    container_name       = "tfstate"
    key                  = "terraform.tfstate"
    use_oidc             = true
  }
}

provider "azurerm" {
  features {}
  use_oidc = true
}

# The Databricks provider will use Azure AD (Entra ID) credentials from the azurerm provider
provider "databricks" {
  # host = azurerm_databricks_workspace.this.workspace_url
  # We will configure this fully once we build the workspace resource in main.tf
}
