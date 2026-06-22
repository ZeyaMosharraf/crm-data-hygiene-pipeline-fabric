# Bronze Layer Documentation

## Overview
The Bronze layer contains raw data ingestion pipelines that pull data directly from HubSpot CRM APIs and store it in its raw, unprocessed form in the data lake.

## Architecture

### Data Sources
- **HubSpot CRM API** (v3)
- Objects ingested: Contacts, Companies, Deals

### Ingestion Modes
1. **Full Sync (Initial Load)**: Fetches all records from inception
2. **Incremental Sync (Subsequent Runs)**: Fetches only records modified after last known timestamp

### Processing Flow
```
Checkpoint Load → Determine Sync Mode → Fetch Data → Save to Bronze → Update Checkpoint
```

## Notebooks

### `bronze_hubspot_contacts_ingestion.Notebook`
Main ingestion orchestrator for HubSpot data.

**Key Functions:**

#### `get_hubspot_session()`
- Creates a resilient HTTP session with retry logic
- Retries on rate limits (429) and server errors (500-504)
- Exponential backoff with factor of 1
- Max 5 retry attempts

**Parameters:** None  
**Returns:** `requests.Session` object with configured retry strategy

#### `get_credentials()`
- Validates and retrieves API credentials from environment
- Raises `ValueError` if credentials are missing

**Parameters:** None  
**Returns:** Dictionary with `access_token` and `base_url`

#### `fetch_object(object_type, after=None, last_updated=None, limit=100)`
Core API fetching function with dual-mode support.

**Modes:**
- **Search API** (when `last_updated` is set):
  - Uses HubSpot's search endpoint
  - Filters by `lastmodified_field > last_updated` (millisecond timestamp)
  - Ideal for incremental loads

- **List API** (default):
  - Uses HubSpot's standard list endpoint
  - Fetches objects without timestamp filtering
  - Ideal for initial full sync

**Parameters:**
- `object_type` (str): "contacts", "companies", or "deals"
- `after` (str, optional): Pagination cursor for fetching next batch
- `last_updated` (str, optional): ISO timestamp for incremental filtering
- `limit` (int): Records per API call (default: 100)

**Returns:** Dictionary containing:
- `data`: List of objects fetched
- `next_after`: Cursor for next batch (None if complete)
- `last_id`: ID of last record in batch
- `last_updated_at`: Latest timestamp in batch
- `batch_size`: Number of records in batch

**Error Handling:**
- Catches all exceptions
- Logs response status and error details
- Logs request body/params for debugging
- Re-raises exception after logging

#### `save_to_bronze(object_type, data)`
Persists fetched data to lakehouse storage.

**Parameters:**
- `object_type` (str): Type of object being saved
- `data` (list): Records to save

**Behavior:**
- Creates directory: `/lakehouse/default/Files/bronze/hubspot/{object_type}/`
- Generates timestamp-based filename: `YYYYMMDD_HHMMSS_microseconds.json`
- Logs record count and save duration
- Silently skips if data is empty

#### `save_checkpoint(object_type, after=None, last_id=None, last_updated_at=None)`
Saves ingestion state for resumability.

**Parameters:**
- `object_type` (str): Type of object
- `after` (str): Last pagination cursor (saved as None on completion)
- `last_id` (str): ID of last record (for reference, not used in incremental logic)
- `last_updated_at` (str): Latest modified timestamp from batch

**Checkpoint Structure:**
```json
{
  "last_after": "pagination_cursor_or_null",
  "last_id": "last_record_id",
  "last_updated_at": "2024-06-22T19:31:38.000Z",
  "last_run_time": "2024-06-22T19:31:38.123456"
}
```

**Location:** `/lakehouse/default/Files/bronze/hubspot/{object_type}/checkpoint/checkpoint.json`

#### `load_checkpoint(object_type)`
Retrieves last known ingestion state.

**Parameters:**
- `object_type` (str): Type of object

**Returns:**
- Dictionary with checkpoint data if exists
- None if checkpoint file doesn't exist

#### `run_ingestion(object_type)`
Main orchestration function for a single object type.

**Logic:**
1. Loads checkpoint (or starts fresh)
2. Loops through paginated results:
   - Fetches next batch
   - Saves to bronze layer
   - Tracks max timestamp across batches
   - Updates checkpoint after each batch
   - Continues until `next_after` is None
3. Outputs summary statistics

**Output Metrics:**
- Total Records: Sum of all records fetched
- Total Batches: Number of API paginations
- Last Updated: Maximum timestamp from all records
- Time Taken: Total execution duration (seconds)
- API Calls: Number of API requests made

#### `run_all_objects()`
Executes ingestion for all defined objects sequentially.

**Objects Processed:**
- contacts
- companies
- deals

## Configuration

### Object Properties (from `hubspot_properties.Notebook`)

```python
hubspot_properties = {
    "contacts": {
        "properties": ["email", "firstname", "lastname", "phone"],
        "lastmodified_field": "lastmodifieddate"
    },
    "companies": {
        "properties": ["name", "domain"],
        "lastmodified_field": "hs_lastmodifieddate"
    },
    "deals": {
        "properties": ["dealname", "amount"],
        "lastmodified_field": "hs_lastmodifieddate"
    }
}
```

**Notes:**
- Each object type has specific properties to fetch
- `lastmodified_field` specifies which field to use for incremental filtering
- Contacts use `lastmodifieddate` while Companies/Deals use `hs_lastmodifieddate`

## Checkpoint Logic

### First Run
1. No checkpoint exists
2. `last_updated` is None → Uses **List API** (full sync)
3. Fetches all records from HubSpot
4. Saves checkpoint with max timestamp

### Subsequent Runs
1. Loads previous checkpoint with timestamp
2. `last_updated` has value → Uses **Search API** (incremental)
3. Filters: `lastmodified_field > last_updated_timestamp`
4. Only fetches records modified since last run
5. Saves updated timestamp for next run
6. `last_id` saved as None (cursor-based pagination resets)

**Why this is efficient:**
- Timestamp-based filtering avoids fetching unchanged records
- Incremental loads reduce API calls and data transfer
- Resumable: Can pick up from last known position if interrupted

## Error Handling

### Retry Logic
- Built into HTTP session via `urllib3.util.retry.Retry`
- Retries up to 5 times with exponential backoff
- Triggers on: Rate limits (429), Server errors (500, 502, 503, 504)
- Allows: GET, POST methods

### Exception Handling
- All exceptions caught in `fetch_object()`
- Logs response status and error text
- Includes request body/params for debugging
- Re-raises to stop ingestion on unrecoverable errors

### Common Issues
- **Missing credentials**: Raises `ValueError` at startup
- **API timeout**: Requests have 30-second timeout
- **Invalid object type**: Raises `ValueError` if properties not defined

## Performance Metrics

### API Efficiency
- Batch size: 100 records per API call (configurable)
- Rate limit: HubSpot allows multiple requests in rapid succession with retry backoff
- Estimated calls for full sync:
  - Contacts: 50-100 calls (5,000-10,000 records typical)
  - Companies: 50-100+ calls (5,000+ records common in large orgs)
  - Deals: Varies widely based on sales org

### File Storage
- Format: JSON (one file per batch)
- Path: `/lakehouse/default/Files/bronze/hubspot/{object_type}/YYYYMMDD_HHMMSS_ffffff.json`
- Checkpoints: `/lakehouse/default/Files/bronze/hubspot/{object_type}/checkpoint/checkpoint.json`

## Dependencies

### Credentials (from `config_credentials` notebook)
- `HUBSPOT_ACCESS_TOKEN`: Private API token from HubSpot
- `BASE_URL`: HubSpot API base URL (typically `https://api.hubapi.com`)

### Libraries
- `requests`: HTTP client with retry support
- `urllib3.util.retry`: Retry strategy configuration
- `json`: Data serialization
- `datetime`: Timestamp handling
- `time`: Performance monitoring
- `os`: File system operations

## Execution

### Manual Execution
```python
run_all_objects()  # Runs contacts, companies, deals sequentially
```

### Scheduled Execution
Notebook can be scheduled via Fabric/Synapse pipelines for recurring ingestion.

### Execution Output
Example console output:
```
Notebook Started
2024-06-22 19:31:38.123456

contacts Search API Time: 1.23 sec
Saved 100 records → 20240622_193138_123456
Checkpoint saved: {"last_after": null, "last_id": "123", "last_updated_at": "2024-06-22T19:30:00Z", "last_run_time": "2024-06-22T19:31:38.654321"}
...

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

## Data Quality Considerations

### Current State
- Raw data: No transformations or cleaning applied
- No deduplication: Same records may appear in multiple files
- Schema: Depends on HubSpot object structure (not validated)
- Timestamps: Uses HubSpot's `lastmodifieddate` for incremental tracking

### Recommendations
- Implement schema validation in silver layer
- Add deduplication logic (merge by object ID)
- Monitor for data drift from HubSpot schema changes
- Consider adding data quality checks (e.g., required fields present)

## Troubleshooting

### Too Many API Calls (5000+)
**Symptoms:** Checkpoint not loading or incremental filter not working
**Solutions:**
1. Verify checkpoint files exist and are readable
2. Check `hs_lastmodifieddate` field exists in HubSpot for companies/deals
3. Increase batch size from 100 to 500 (reduces calls by 80%)
4. Check if timestamps are being persisted correctly

### Missing Data
**Symptoms:** Expected records not appearing in bronze files
**Solutions:**
1. Verify credentials are correct
2. Check if object type is in the defined list (contacts, companies, deals)
3. Ensure HubSpot user has permission to access object types
4. Check API response for error messages in logs

### Slow Execution
**Symptoms:** Notebook taking >5 minutes for typical run
**Solutions:**
1. Increase batch size to 500
2. Check network connectivity to HubSpot API
3. Monitor for rate limiting (429 responses) in logs
4. Run object ingestions in parallel (requires notebook refactoring)

## Future Enhancements

1. **Parallel Processing**: Execute contacts, companies, deals concurrently
2. **Dynamic Properties**: Load properties from configuration table instead of hardcoding
3. **Data Quality Framework**: Add validation checks before saving to bronze
4. **Notification System**: Alert on ingestion failures or anomalies
5. **Incremental Optimization**: Profile to determine optimal batch sizes
6. **Deduplication**: Merge duplicate records within same ingestion run
