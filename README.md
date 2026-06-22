# CRM Data Hygiene Pipeline - Fabric

A comprehensive data pipeline for ingesting, transforming, and cleaning HubSpot CRM data using the medallion architecture pattern in Microsoft Fabric.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Current Implementation](#current-implementation)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Performance](#performance)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)
- [Documentation](#documentation)

## 🎯 Overview

This project implements a **medallion architecture** data pipeline (Bronze → Silver → Gold) to:

1. **Ingest** raw HubSpot CRM data (Contacts, Companies, Deals)
2. **Transform** and clean data in silver layer
3. **Aggregate** business metrics in gold layer
4. **Enable** data-driven insights for CRM analytics

### Key Features

✅ **Incremental Loading**: Only fetches records modified since last run  
✅ **Fault Tolerant**: Built-in retry logic with exponential backoff  
✅ **Resumable**: Checkpoint-based state management for interrupted runs  
✅ **Observable**: Detailed logging and performance metrics  
✅ **Scalable**: Designed for large datasets (5000+ records)  
✅ **Production Ready**: Error handling and validation throughout

## 🏗️ Architecture

### Medallion Pattern

```
Bronze Layer (Raw Data)
    ↓
    └─→ Raw HubSpot data ingestion
        • Contacts, Companies, Deals
        • No transformations
        • JSON file format
        • Timestamp-based incremental sync

Silver Layer (Cleaned Data)
    ↓
    └─→ Data quality & transformation
        • Schema validation
        • Deduplication
        • Type standardization
        • (In Development)

Gold Layer (Business Analytics)
    ↓
    └─→ Aggregated metrics & insights
        • Business KPIs
        • Dashboard data
        • Reports
        • (In Development)
```

### Data Flow

```
HubSpot CRM API
        ↓
   Fabric Notebook
   (Ingestion)
        ↓
  Bronze Layer
  (Raw Files)
        ↓
  Silver Layer
  (Transformed)
        ↓
  Gold Layer
  (Analytics)
```

## 📁 Project Structure

```
crm-data-hygiene-pipeline-fabric/
├── 00_shared/
│   └── hubspot_properties.Notebook/
│       └── notebook-content.py       # Object property definitions
│
├── 01_bronze/
│   └── bronze_hubspot_contacts_ingestion.Notebook/
│       └── notebook-content.py       # Main ingestion orchestrator
│
├── 02_silver/
│   └── (In Development)
│
├── 03_gold/
│   └── (In Development)
│
├── 04_pipelines/
│   └── (In Development)
│
├── docs/
│   ├── bronze/
│   │   ├── bronze_layer_documentation.md
│   │   └── architecture/
│   │       └── architecture.md
│   └── readme.md (this file)
│
└── bronze_lakehouse.Lakehouse
    └── (Fabric Lakehouse Configuration)
```

## 🔧 Current Implementation

### Bronze Layer - Ingestion

**Status:** ✅ Production Ready

**Components:**
- `config_credentials`: Environment-based credential management
- `hubspot_properties`: Object-level property definitions
- `bronze_hubspot_contacts_ingestion`: Main orchestrator notebook

**Supported Objects:**
- Contacts (25 properties)
- Companies (20 properties)
- Deals (15 properties)

**Ingestion Modes:**
1. **Full Sync**: Initial complete data load
2. **Incremental Sync**: Fetches only modified records

**Features:**
- Automatic retry with exponential backoff (5 attempts)
- Pagination handling (configurable batch size: 100 records)
- Timestamp-based state tracking
- Resumable from last checkpoint
- Detailed execution metrics

**Performance:**
- Contacts: ~50-100 API calls per full sync
- Companies: ~50-100+ API calls (high variance)
- Deals: ~20-40 API calls
- Average runtime: 2-6 minutes per full execution

### Silver Layer

**Status:** 🔨 In Development

**Planned Features:**
- Schema validation
- Deduplication logic
- Data type standardization
- Data quality metrics
- Null/empty field handling

### Gold Layer

**Status:** 🔨 In Development

**Planned Features:**
- Business KPIs
- Contact engagement metrics
- Deal pipeline analytics
- Company health scores
- Custom dashboards

### Pipelines

**Status:** 🔨 In Development

**Planned Features:**
- Scheduled ingestion (hourly/daily)
- Error notifications
- Data quality monitoring
- Performance tracking

## 🚀 Getting Started

### Prerequisites

- Microsoft Fabric Workspace
- HubSpot API Access Token
- Python 3.8+ (for local development)
- Git

### Setup

#### 1. Configure Credentials

Create or update the `config_credentials` notebook with:

```python
HUBSPOT_ACCESS_TOKEN = "<your-hubspot-private-app-token>"
BASE_URL = "https://api.hubapi.com"
```

**Note:** Never commit credentials to repository. Use Fabric workspace variables or Azure Key Vault.

#### 2. Verify Lakehouse

Ensure lakehouse is configured:
```
bronze_lakehouse (default)
├── Files/
│   └── bronze/
│       └── hubspot/
```

#### 3. Run Initial Ingestion

Execute the notebook:
```python
# In: bronze_hubspot_contacts_ingestion.Notebook
run_all_objects()  # Runs contacts, companies, deals
```

Expected output:
```
Notebook Started
2024-06-22 19:31:38.123456

contacts Search API Time: 1.23 sec
Saved 100 records → 20240622_193138_123456
Checkpoint saved: {...}

========================================
Ingestion Completed: contacts
========================================
Total Records : 5000
Total Batches : 50
Last Updated  : 2024-06-22T19:30:00.000Z
Time Taken    : 45.67 sec
========================================
API Calls     : 50
```

## 📖 Usage

### Manual Execution

#### Ingest All Objects
```python
run_all_objects()
```

#### Ingest Single Object
```python
run_ingestion("contacts")
# OR
run_ingestion("companies")
# OR
run_ingestion("deals")
```

### Scheduled Execution

Set up a Fabric pipeline to run the notebook on a schedule:

```yaml
Trigger: Daily at 2:00 AM UTC
Notebook: bronze_hubspot_contacts_ingestion
Timeout: 10 minutes
Retry: 3 attempts
```

### Incremental vs Full Sync

**Automatic Selection:**
- **First Run**: Uses full sync (no checkpoint exists)
- **Subsequent Runs**: Uses incremental sync (only changed records)

**Force Full Sync:**
```python
# Delete checkpoint file manually, then run
import os
checkpoint_path = "/lakehouse/default/Files/bronze/hubspot/contacts/checkpoint/checkpoint.json"
if os.path.exists(checkpoint_path):
    os.remove(checkpoint_path)

run_ingestion("contacts")
```

### Data Access

#### Query Bronze Data
```sql
-- Via Lakehouse SQL view
SELECT * 
FROM bronze.hubspot_contacts_raw
WHERE created_date >= CURRENT_DATE - 30
```

#### Browse Files
```
Lakehouse → Files → bronze → hubspot → {object_type} → *.json
```

## 📊 Performance

### API Efficiency

| Object | Records | API Calls | Duration | Records/Call |
|--------|---------|-----------|----------|--------------|
| Contacts | 5,000-10,000 | 50-100 | 50-100s | 100 |
| Companies | 5,000-20,000 | 50-100+ | 50-200s | 100 |
| Deals | 1,000-5,000 | 20-40 | 20-40s | 100 |

**⚠️ Note:** Company data shows high variance (5000+ calls reported). See [Troubleshooting](#troubleshooting).

### Optimization Tips

1. **Increase Batch Size** (Quick Win)
   ```python
   # Change in fetch_object() function
   def fetch_object(object_type, after=None, last_updated=None, limit: int = 500):
   ```
   Expected improvement: 80% reduction in API calls

2. **Parallel Execution**
   - Run contacts, companies, deals concurrently (requires refactoring)
   - Estimated improvement: 3x faster

3. **Filter Properties**
   - Request only needed fields to reduce payload
   - Update `hubspot_properties.Notebook`

4. **Monitor Checkpoints**
   - Verify checkpoints are saved/loaded correctly
   - Prevents unnecessary full syncs

## 🐛 Troubleshooting

### Too Many API Calls (5000+)

**Symptoms:**
- Company ingestion making excessive API calls
- Slower than expected execution
- High API costs

**Root Causes:**
- Checkpoint not loading (falls back to full sync every run)
- Incremental filter field not working for companies
- Timestamp precision issues

**Solutions:**

1. **Verify Checkpoint Exists**
   ```python
   checkpoint = load_checkpoint("companies")
   print(f"Checkpoint: {checkpoint}")  # Should show last_updated_at
   ```

2. **Check Checkpoint Path**
   ```
   Lakehouse → Files → bronze → hubspot → companies → checkpoint → checkpoint.json
   ```

3. **Increase Batch Size to 500**
   - Reduces calls proportionally
   - Current: 100 recs/call → New: 500 recs/call

4. **Force Full Sync Then Monitor**
   - Delete checkpoint
   - Run full sync
   - Verify incremental works on next run

### Missing Data

**Symptoms:**
- Expected records not in bronze files
- Fewer records than expected

**Causes:**
- Invalid credentials
- Insufficient HubSpot permissions
- Object type not defined in properties
- API filter not matching any records

**Solutions:**
```python
# Test credentials
creds = get_credentials()
print(f"Token: {creds['access_token'][:10]}...")
print(f"Base URL: {creds['base_url']}")

# Test connectivity
session = get_hubspot_session()
response = session.get(f"{creds['base_url']}/contacts/v1/lists/all/contacts/all")
print(f"Status: {response.status_code}")
```

### Slow Execution

**Symptoms:**
- Notebook taking >10 minutes
- Network latency evident

**Causes:**
- Network bandwidth issues
- HubSpot API rate limiting
- High load on Fabric cluster
- Large payloads

**Solutions:**
- Run during off-peak hours
- Reduce batch size temporarily
- Check HubSpot API dashboard for rate limits
- Verify network connectivity

### Checkpoint Not Updating

**Symptoms:**
- Same data fetched repeatedly
- Checkpoint file exists but not updating

**Solutions:**
```python
# Check file permissions
import os
path = "/lakehouse/default/Files/bronze/hubspot/contacts/checkpoint/checkpoint.json"
print(f"Path exists: {os.path.exists(path)}")
print(f"Is writable: {os.access(path, os.W_OK)}")

# Manually verify checkpoint structure
with open(path, 'r') as f:
    import json
    data = json.load(f)
    print(json.dumps(data, indent=2))
```

## 🗺️ Roadmap

### Phase 1: Bronze Layer ✅
- [x] HubSpot API integration
- [x] Full sync capability
- [x] Incremental sync capability
- [x] Checkpoint-based resumability
- [x] Error handling & logging

### Phase 2: Silver Layer 🔨
- [ ] Schema validation
- [ ] Deduplication logic
- [ ] Data type standardization
- [ ] Data quality scoring
- [ ] Audit trail

### Phase 3: Gold Layer 🔨
- [ ] Business KPIs
- [ ] Contact engagement metrics
- [ ] Deal pipeline analytics
- [ ] Company health scoring
- [ ] Custom dashboards

### Phase 4: Operations 🔨
- [ ] Scheduled pipelines
- [ ] Failure notifications
- [ ] Performance monitoring
- [ ] Data quality alerts
- [ ] Cost optimization

### Phase 5: Enhancements 🔨
- [ ] Support additional HubSpot objects (Tickets, Tasks, Calls)
- [ ] Real-time ingestion (webhooks)
- [ ] Custom transformations
- [ ] Data lineage tracking
- [ ] Multi-workspace support

## 📚 Documentation

For detailed technical documentation, see:

- **[bronze_layer_documentation.md](docs/bronze/bronze_layer_documentation.md)**
  - Complete function reference
  - Configuration details
  - Error handling guide
  - Performance metrics

- **[architecture.md](docs/bronze/architecture/architecture.md)**
  - System diagrams
  - Data flow architecture
  - Component breakdown
  - State machines
  - Scalability analysis

## 🤝 Contributing

### Code Style
- Follow PEP 8 for Python
- Use descriptive variable names
- Add comments for complex logic
- Include docstrings for functions

### Testing
- Test on dev workspace before deploying to production
- Verify checkpoint behavior after changes
- Monitor API call counts
- Check data quality

### Reporting Issues
When reporting issues, include:
1. Object type (contacts, companies, deals)
2. Approximate record count
3. API call count
4. Error message from logs
5. Checkpoint file content

## 📋 Prerequisites for Deployment

- [ ] Credentials configured in Fabric
- [ ] Lakehouse created and configured
- [ ] HubSpot API token with correct permissions
- [ ] Fabric workspace access
- [ ] Sufficient storage quota (estimate: 1GB per 100K records)

## 🔒 Security

### Best Practices

1. **Never commit credentials**
   - Use Fabric workspace variables
   - Use Azure Key Vault for sensitive data
   - Rotate tokens regularly

2. **Limit permissions**
   - Service principal with minimum required scopes
   - Read-only access to HubSpot API

3. **Audit access**
   - Enable Fabric audit logging
   - Monitor checkpoint modifications
   - Track pipeline executions

## 📞 Support

For issues or questions:
1. Check [Troubleshooting](#troubleshooting) section
2. Review detailed [documentation](docs/bronze)
3. Check HubSpot API documentation
4. Review notebook logs for error messages

## 📄 License

This project is licensed under MIT License.

## 🙏 Acknowledgments

- Built with Microsoft Fabric
- HubSpot CRM API
- Open-source Python libraries (requests, urllib3)

---

**Last Updated:** 2024-06-22  
**Version:** 1.0.0 (Bronze Layer)  
**Status:** Production Ready
