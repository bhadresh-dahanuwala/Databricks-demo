# Databricks CLI Reference & Frequently Used Commands

A practical reference guide for frequently used commands in the modern Databricks CLI (v1.x+), including command syntax, usage scenarios, and useful options/flags.

---

## Table of Contents
1. [Global Flags](#global-flags)
2. [Authentication & Profiles](#1-authentication--profiles)
3. [Databricks Asset Bundles (DABs)](#2-databricks-asset-bundles-dabs)
4. [Jobs & Workflows](#3-jobs--workflows)
5. [Pipelines (Lakeflow / Delta Live Tables)](#4-pipelines-lakeflow--delta-live-tables)
6. [Compute & Clusters](#5-compute--clusters)
7. [SQL Warehouses](#6-sql-warehouses)
8. [Unity Catalog](#7-unity-catalog)
9. [Workspace Objects](#8-workspace-objects)
10. [Filesystem & DBFS / Volumes (`fs`)](#9-filesystem--dbfs--volumes-fs)
11. [Secrets Management](#10-secrets-management)
12. [Direct REST API Calls (`api`)](#11-direct-rest-api-calls-api)

---

## Global Flags

These flags can be appended to almost any Databricks CLI command:

| Flag | Shorthand | Description |
| :--- | :--- | :--- |
| `--output <type>` | `-o json` / `-o text` | Format command output as `json` (default is human-readable `text`). Useful for scripting and piping to `jq`. |
| `--profile <name>` | `-p <name>` | Select the connection profile configured in `~/.databrickscfg`. |
| `--target <target>`| `-t <target>` | Specify the bundle target environment (e.g., `dev`, `staging`, `prod`). |
| `--debug` | | Enable verbose debug logging (displays HTTP headers and request/response payloads). |
| `--help` | `-h` | Display command help and available arguments. |

---

## 1. Authentication & Profiles

### List configured profiles
* **Usage**: View all workspace profiles available in `~/.databrickscfg`.
* **Command**:
  ```bash
  databricks auth profiles
  ```
* **Useful Options**:
  * `-o json` – Output profile details as JSON.

### Describe active credentials
* **Usage**: Validate authentication status, token validity, and current host identity.
* **Command**:
  ```bash
  databricks auth describe
  ```
* **Useful Options**:
  * `-p <profile>` – Check authentication for a specific profile.

### Log in via OAuth / browser
* **Usage**: Authenticate to a Databricks workspace using OAuth U2M (User-to-Machine) in your browser.
* **Command**:
  ```bash
  databricks auth login --host https://<workspace-instance>.cloud.databricks.com
  ```
* **Useful Options**:
  * `--profile <name>` – Name the profile created in `~/.databrickscfg`.
  * `--account-id <id>` – Use for Databricks account-level authentication.

### Interactive profile configuration
* **Usage**: Interactively set up a Personal Access Token (PAT) or OAuth profile.
* **Command**:
  ```bash
  databricks configure
  ```
* **Useful Options**:
  * `-p <profile>` – Specify or overwrite a named profile.

---

## 2. Databricks Asset Bundles (DABs)

### Validate bundle configuration
* **Usage**: Verify syntax and reference validity in `databricks.yml` without deploying.
* **Command**:
  ```bash
  databricks bundle validate
  ```
* **Useful Options**:
  * `-t <target>` – Target environment to evaluate variables for (e.g., `-t dev` or `-t prod`).
  * `--var "<key>=<val>"` – Override bundle variable values dynamically.

### Deploy bundle resources
* **Usage**: Deploy jobs, pipelines, and artifacts defined in the bundle to the target workspace.
* **Command**:
  ```bash
  databricks bundle deploy -t dev
  ```
* **Useful Options**:
  * `-t <target>` – Deploy to a specific target (e.g., `dev`, `prod`).
  * `--auto-approve` – Skip interactive prompts (essential for CI/CD pipelines).
  * `--force` – Force deployment bypassing Git branch validation checks.
  * `--select <resource>` – Deploy only a specific resource (e.g., `--select my_job`).
  * `-c, --cluster-id <id>` – Override compute cluster ID for the deployment.

### Run a bundle resource
* **Usage**: Trigger an execution of a job, pipeline, or app declared in your bundle.
* **Command**:
  ```bash
  databricks bundle run <resource_key>
  ```
* **Useful Options**:
  * `-t <target>` – Specify target environment.
  * `--no-wait` – Trigger the run without waiting for completion.
  * `--restart` – Restart the run if one is already running.
  * `--full-refresh-all` – When running a pipeline, force recomputation of the entire graph.
  * `--params "<key>=<val>"` – Pass runtime job parameters.

### Destroy bundle resources
* **Usage**: Tear down and delete all resources deployed by the bundle in the specified target.
* **Command**:
  ```bash
  databricks bundle destroy -t dev
  ```
* **Useful Options**:
  * `--auto-approve` – Destroy without interactive confirmation prompt.

---

## 3. Jobs & Workflows

### List jobs
* **Usage**: List workflow jobs defined in the workspace.
* **Command**:
  ```bash
  databricks jobs list
  ```
* **Useful Options**:
  * `--name "<job-name>"` – Filter by exact job name (case-insensitive).
  * `--limit <int>` – Limit maximum number of returned results.
  * `--offset <int>` – Offset index for pagination.
  * `--expand-tasks` – Include task and cluster configuration details.
  * `-o json` – Output as JSON.

### Get job details
* **Usage**: Retrieve configuration, task DAG, and metadata for a specific job.
* **Command**:
  ```bash
  databricks jobs get <job-id>
  ```

### Trigger a job run
* **Usage**: Manually start an immediate run of a job.
* **Command**:
  ```bash
  databricks jobs run-now <job-id>
  ```
* **Useful Options**:
  * `--job-parameters '{"key": "value"}'` – Pass JSON-formatted job parameters.
  * `--no-wait` – Return run ID immediately instead of waiting for execution to finish.

### List job runs
* **Usage**: View execution history and active runs.
* **Command**:
  ```bash
  databricks jobs list-runs
  ```
* **Useful Options**:
  * `--job-id <job-id>` – Filter runs belonging to a specific job.
  * `--active-only` – Show only currently executing runs.
  * `--completed-only` – Show only finished runs.
  * `--limit <int>` – Limit result count.

### Cancel a running job
* **Usage**: Cancel an active job run.
* **Command**:
  ```bash
  databricks jobs cancel-run <run-id>
  ```

---

## 4. Pipelines (Lakeflow / Delta Live Tables)

> **Important Note:** In the Databricks CLI, the command to list pipelines is `list-pipelines`, not `list`.

### List pipelines
* **Usage**: List Delta Live Tables pipelines in the workspace.
* **Command**:
  ```bash
  databricks pipelines list-pipelines
  ```
* **Useful Options**:
  * `--filter "name LIKE '<pattern>%'"` – Filter results with SQL-like syntax.
  * `--max-results <int>` – Maximum entries returned per page.
  * `-o json` – Output raw JSON structure.

### Get pipeline details
* **Usage**: Retrieve definition, settings, and cluster specs of a pipeline.
* **Command**:
  ```bash
  databricks pipelines get <pipeline-id>
  ```

### Start pipeline update
* **Usage**: Trigger execution of a pipeline.
* **Command**:
  ```bash
  databricks pipelines start-update <pipeline-id>
  ```
* **Useful Options**:
  * `--full-refresh` – Reset and recompute all tables from scratch.
  * `--refresh-selection <table1,table2>` – Recompute specific tables only.
  * `--validate-only` – Validate code and graph syntax without running updates.

### Stop a pipeline
* **Usage**: Halt an ongoing pipeline update.
* **Command**:
  ```bash
  databricks pipelines stop <pipeline-id>
  ```

### View pipeline events / logs
* **Usage**: View execution logs, data quality expectations, and error messages.
* **Command**:
  ```bash
  databricks pipelines list-pipeline-events <pipeline-id>
  ```
* **Useful Options**:
  * `--max-results <int>` – Max event records to retrieve.

---

## 5. Compute & Clusters

### List clusters
* **Usage**: List all-purpose and job clusters in the workspace.
* **Command**:
  ```bash
  databricks clusters list
  ```
* **Useful Options**:
  * `-o json` – Output cluster list with full specs in JSON format.

### Get cluster information
* **Usage**: Inspect cluster status, node types, Spark version, and spark_conf.
* **Command**:
  ```bash
  databricks clusters get <cluster-id>
  ```

### Start / Restart a cluster
* **Usage**: Start a terminated cluster or reboot an active one.
* **Command**:
  ```bash
  databricks clusters start <cluster-id>
  databricks clusters restart <cluster-id>
  ```

### Terminate (delete) a cluster
* **Usage**: Stop an active cluster (can be restarted later within 30 days or if pinned).
* **Command**:
  ```bash
  databricks clusters delete <cluster-id>
  ```

---

## 6. SQL Warehouses

### List SQL warehouses
* **Usage**: View all serverless and classic SQL endpoints.
* **Command**:
  ```bash
  databricks warehouses list
  ```
* **Useful Options**:
  * `-o json` – Get warehouse details (size, state, channel, auto-stop mins).

### Start or stop a warehouse
* **Usage**: Manually spin up or suspend a SQL warehouse to save DBUs.
* **Command**:
  ```bash
  databricks warehouses start <warehouse-id>
  databricks warehouses stop <warehouse-id>
  ```

---

## 7. Unity Catalog

### Catalogs
* **Usage**: List or inspect top-level Unity Catalog containers.
* **Commands**:
  ```bash
  # List catalogs
  databricks catalogs list

  # Get catalog details
  databricks catalogs get <catalog-name>
  ```

### Schemas (Databases)
* **Usage**: List schemas within a given catalog.
* **Commands**:
  ```bash
  databricks schemas list <catalog-name>
  databricks schemas get <catalog-name>.<schema-name>
  ```

### Tables
* **Usage**: List tables inside a specific catalog and schema.
* **Commands**:
  ```bash
  databricks tables list <catalog-name> <schema-name>
  databricks tables get <catalog-name>.<schema-name>.<table-name>
  ```

### Volumes
* **Usage**: List Unity Catalog volumes for file and unstructured data governance.
* **Commands**:
  ```bash
  databricks volumes list <catalog-name> <schema-name>
  databricks volumes read <catalog-name>.<schema-name>.<volume-name>
  ```

---

## 8. Workspace Objects

### List workspace folder contents
* **Usage**: List notebooks, directories, and files in the workspace tree.
* **Command**:
  ```bash
  databricks workspace list /Users/<user-email>/
  ```
* **Useful Options**:
  * `--absolute-path` – Return fully-qualified paths.

### Import a local file or notebook
* **Usage**: Upload a local script, SQL file, or notebook into the workspace.
* **Command**:
  ```bash
  databricks workspace import /path/to/local/notebook.py /Workspace/Users/<email>/notebook
  ```
* **Useful Options**:
  * `--format <SOURCE|HTML|JUPYTER|DBC|AUTO>` – Specify file format.
  * `--language <PYTHON|SQL|SCALA|R>` – Specify language for source code files.
  * `--overwrite` – Overwrite the workspace object if it already exists.

### Export a workspace notebook or file
* **Usage**: Download a notebook or file to the local filesystem.
* **Command**:
  ```bash
  databricks workspace export /Workspace/Users/<email>/notebook /path/to/local/notebook.py
  ```
* **Useful Options**:
  * `--format <SOURCE|HTML|JUPYTER|DBC>` – Export format (e.g. `JUPYTER` for `.ipynb`).

### Sync entire directory to workspace
* **Usage**: Bulk import an entire directory tree.
* **Command**:
  ```bash
  databricks workspace import-dir ./local-folder /Workspace/Users/<email>/remote-folder
  ```

---

## 9. Filesystem & DBFS / Volumes (`fs`)

`databricks fs` supports DBFS URIs (`dbfs:/...`) as well as Unity Catalog Volumes (`/Volumes/<catalog>/<schema>/<volume>/...`).

### List files
* **Usage**: List files and subdirectories.
* **Command**:
  ```bash
  databricks fs ls /Volumes/<catalog>/<schema>/<volume>/
  # Or legacy DBFS:
  databricks fs ls dbfs:/FileStore/
  ```
* **Useful Options**:
  * `--absolute-path` – Return absolute paths.

### Copy files (`cp`)
* **Usage**: Upload local files to Databricks or download remote files locally.
* **Commands**:
  ```bash
  # Upload local file to Volume
  databricks fs cp ./data.csv /Volumes/<catalog>/<schema>/<volume>/data.csv

  # Download remote file to local directory
  databricks fs cp /Volumes/<catalog>/<schema>/<volume>/data.csv ./data.csv
  ```
* **Useful Options**:
  * `-r, --recursive` – Recursively copy whole directories.
  * `--overwrite` – Overwrite destination files if they exist.

### Display file contents (`cat`)
* **Usage**: Read the head/content of a file stored in Volumes or DBFS.
* **Command**:
  ```bash
  databricks fs cat /Volumes/<catalog>/<schema>/<volume>/sample.txt
  ```

---

## 10. Secrets Management

### List secret scopes
* **Usage**: View all existing secret scopes.
* **Command**:
  ```bash
  databricks secrets list-scopes
  ```

### Create a secret scope
* **Usage**: Create a new scope for isolating keys and credentials.
* **Command**:
  ```bash
  databricks secrets create-scope <scope-name>
  ```
* **Useful Options**:
  * `--initial-manage-principal <users|creator>` – Initial principal with MANAGE permission.

### Store a secret
* **Usage**: Save a secret key-value pair into a scope.
* **Command**:
  ```bash
  # Prompt interactively for secret value (secure, avoids shell history)
  databricks secrets put-secret <scope-name> <secret-key>

  # Or provide via flag:
  databricks secrets put-secret <scope-name> <secret-key> --string-value "<my-secret-token>"
  ```

### List secret keys in a scope
* **Usage**: View key names in a scope (secret values are never displayed).
* **Command**:
  ```bash
  databricks secrets list-secrets <scope-name>
  ```

### Delete a secret
* **Usage**: Remove a secret from a scope.
* **Command**:
  ```bash
  databricks secrets delete-secret <scope-name> <secret-key>
  ```

---

## 11. Direct REST API Calls (`api`)

When a feature or parameter is not directly wrapped by a sub-command, the `databricks api` command can execute authenticated calls to any Databricks REST API endpoint.

### GET request
* **Usage**: Call any GET endpoint using the CLI's active authentication.
* **Command**:
  ```bash
  databricks api get /api/2.1/jobs/list
  ```
* **Useful Options**:
  * `--profile <name>` – Use specific profile.

### POST request
* **Usage**: Send JSON payload to a POST endpoint.
* **Command**:
  ```bash
  databricks api post /api/2.0/clusters/start --json '{"cluster_id": "<cluster-id>"}'
  ```
