# Bronze Layer Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     HubSpot CRM (External)                       │
│  Contacts | Companies | Deals (Raw Data Source)                 │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │  HubSpot API v3              │
        │  (REST Endpoints)            │
        │  - List API                  │
        │  - Search API                │
        └──────────────┬───────────────┘
                       │
                       ▼
    ┌──────────────────────────────────────┐
    │  bronze_hubspot_contacts_ingestion   │
    │  (Fabric Notebook / PySpark)         │
    │                                      │
    │  • Credential Management            │
    │  • Dual-mode API Fetching           │
    │  • Resilient Retry Logic            │
    │  • Checkpoint State Management      │
    │  • Batch Processing & Pagination    │
    └──────────────┬───────────────────────┘
                   │
        ┌──────────┴──────────┬──────────┐
        ▼                     ▼          ▼
    ┌────────────┐    ┌──────────────┐  ┌──────────┐
    │ Contacts   │    │  Companies   │  │  Deals   │
    │ (Ingestion)│    │ (Ingestion)  │  │(Ingestion)
    └──────┬─────┘    └──────┬───────┘  └─────┬────┘
           │                 │                 │
           ▼                 ▼                 ▼
    ┌─────────────────────────────────────────────┐
    │  Bronze Lake (Lakehouse Files)              │
    │                                             │
    │  /lakehouse/default/Files/bronze/hubspot/   │
    │  ├── contacts/                              │
    │  │   ├── 20240622_193138_000000.json        │
    │  │   ├── 20240622_193139_000000.json        │
    │  │   └── checkpoint/checkpoint.json         │
    │  ├── companies/                             │
    │  │   ├── 20240622_193200_000000.json        │
    │  │   └── checkpoint/checkpoint.json         │
    │  └── deals/                                 │
    │      ├── 20240622_193220_000000.json        │
    │      └── checkpoint/checkpoint.json         │
    └─────────────────────────────────────────────┘
```

## Data Flow Architecture

### First Ingestion (Full Sync)
```
Step 1: Load Checkpoint
  └─→ No checkpoint exists (first run)
       └─→ last_updated = None

Step 2: Determine Fetch Mode
  └─→ last_updated is None
       └─→ Use List API (full sync)

Step 3: Fetch Data
  └─→ GET /crm/v3/objects/{object_type}
       └─→ Fetch all records (paginated)

Step 4: Save to Bronze
  └─→ Create /lakehouse/default/Files/bronze/hubspot/{object_type}/
       └─→ Save batch as YYYYMMDD_HHMMSS_ffffff.json

Step 5: Update Checkpoint
  └─→ Save max_updated timestamp
       └─→ Ready for incremental sync on next run
```

### Subsequent Ingestion (Incremental Sync)
```
Step 1: Load Checkpoint
  └─→ Checkpoint exists
       └─→ last_updated = "2024-06-22T19:00:00Z"

Step 2: Determine Fetch Mode
  └─→ last_updated has value
       └─→ Use Search API (incremental)

Step 3: Fetch Data
  └─→ POST /crm/v3/objects/{object_type}/search
       └─→ Filter: lastmodified_field > last_updated (in milliseconds)
           └─→ Only records changed since last run

Step 4: Save to Bronze
  └─→ Save batch as YYYYMMDD_HHMMSS_ffffff.json
       └─→ Avoids re-ingesting unchanged records

Step 5: Update Checkpoint
  └─→ Save new max_updated timestamp
       └─→ Ready for next incremental sync
```

## Component Architecture

### 1. Session Management Layer
```
┌─────────────────────────────────────┐
│   get_hubspot_session()             │
├─────────────────────────────────────┤
│ Creates Resilient HTTP Session      │
│                                     │
│ • Retry Strategy:                   │
│   - Max Retries: 5                  │
│   - Backoff Factor: 1 (exponential) │
│   - Status Codes: 429, 500-504      │
│   - Methods: GET, POST              │
│                                     │
│ • Adapter Configuration:            │
│   - HTTPS: HTTPAdapter              │
│   - HTTP: HTTPAdapter               │
│                                     │
│ Returns: requests.Session object    │
└─────────────────────────────────────┘
```

### 2. Authentication Layer
```
┌──────────────────────────────────────┐
│   get_credentials()                  │
├──────────────────────────────────────┤
│ Validate & Retrieve API Credentials  │
│                                      │
│ Inputs (from Environment):           │
│ • HUBSPOT_ACCESS_TOKEN              │
│ • BASE_URL                          │
│                                      │
│ Validation:                          │
│ • Raises ValueError if missing       │
│                                      │
│ Returns: {"access_token": "...",    │
│           "base_url": "..."}        │
└──────────────────────────────────────┘
```

### 3. API Fetch Layer (Dual-Mode)
```
┌─────────────────────────────────────────────┐
│         fetch_object()                      │
├─────────────────────────────────────────────┤
│                                             │
│  MODE 1: List API (Full Sync)              │
│  ├─ Condition: last_updated is None        │
│  ├─ Endpoint: GET /crm/v3/objects/{type}   │
│  ├─ Params: limit, properties, after       │
│  └─ Use Case: Initial full load            │
│                                             │
│  MODE 2: Search API (Incremental)          │
│  ├─ Condition: last_updated has timestamp  │
│  ├─ Endpoint: POST /crm/v3/objects/{}/     │
│  │            search                        │
│  ├─ Body: filterGroups, properties, limit  │
│  ├─ Filter: {lastmodified_field} > {ms}    │
│  └─ Use Case: Incremental sync             │
│                                             │
│  Returns: {                                 │
│    "data": [...],                          │
│    "next_after": "cursor",                 │
│    "last_id": "id",                        │
│    "last_updated_at": "timestamp",         │
│    "batch_size": 100                       │
│  }                                          │
└─────────────────────────────────────────────┘
```

### 4. Storage Layer
```
┌────────────────────────────────────────┐
│   save_to_bronze()                     │
├────────────────────────────────────────┤
│ Persist Fetched Data                   │
│                                        │
│ Path: /lakehouse/default/Files/bronze/ │
│       hubspot/{object_type}/           │
│                                        │
│ Filename: YYYYMMDD_HHMMSS_ffffff.json │
│ (Microsecond precision for uniqueness) │
│                                        │
│ Operations:                            │
│ • Create directory if missing          │
│ • Write data as JSON                   │
│ • Log record count & duration          │
└────────────────────────────────────────┘
```

### 5. State Management Layer
```
┌──────────────────────────────────────┐
│   Checkpoint System                  │
├──────────────────────────────────────┤
│                                      │
│  save_checkpoint()                   │
│  ├─ Saves state after each batch     │
│  ├─ Location: .../checkpoint.json    │
│  └─ Enables resumability             │
│                                      │
│  load_checkpoint()                   │
│  ├─ Loads previous state             │
│  ├─ Returns None if first run        │
│  └─ Used to resume or skip           │
│                                      │
│  Checkpoint Structure:               │
│  {                                   │
│    "last_after": "cursor|null",      │
│    "last_id": "object_id",           │
│    "last_updated_at": "timestamp",   │
│    "last_run_time": "iso_datetime"   │
│  }                                   │
└──────────────────────────────────────┘
```

### 6. Orchestration Layer
```
┌────────────────────────────────────────┐
│   run_ingestion(object_type)           │
├────────────────────────────────────────┤
│                                        │
│ Main Orchestrator per Object Type      │
│                                        │
│ Loop:                                  │
│  1. Load checkpoint                    │
│  2. Fetch next batch                   │
│  3. Save to bronze                     │
│  4. Track max timestamp                │
│  5. Update checkpoint                  │
│  6. Continue until no more data        │
│                                        │
│ Outputs:                               │
│ • Total Records                        │
│ • Total Batches                        │
│ • Last Updated                         │
│ • Time Taken                           │
│ • API Calls                            │
└────────────────────────────────────────┘
```

## State Machine: Ingestion Workflow

```
┌─────────────────┐
│    START        │
└────────┬────────┘
         │
         ▼
┌──────────────────────┐
│ Load Checkpoint?     │
├──────────────────────┤
│ • Check file exists  │
│ • Parse JSON         │
└────────┬─────────────┘
         │
         ├─── EXISTS ──────┐
         │                 │
         ├─── NOT EXISTS ──┤
         │                 │
         ▼                 ▼
    ┌─────────┐       ┌──────────────┐
    │ Restore │       │ Start Fresh  │
    │ State   │       │              │
    │ From CP │       │ last_updated │
    └────┬────┘       │ = None       │
         │            └──────┬───────┘
         │                   │
         ▼                   ▼
    ┌────────────────────────────────┐
    │ Determine Fetch Mode           │
    ├────────────────────────────────┤
    │ IF last_updated:               │
    │  └─→ USE SEARCH API            │
    │      (Incremental)             │
    │ ELSE:                          │
    │  └─→ USE LIST API              │
    │      (Full Sync)               │
    └────────┬───────────────────────┘
             │
             ▼
    ┌───────────────────┐
    │ Fetch API Batch   │
    │ (with retry)      │
    └────────┬──────────┘
             │
             ├─── HAS DATA ──┐
             │               │
             ├─── NO DATA ───┤
             │               │
             ▼               ▼
    ┌────────────────┐   ┌─────────────┐
    │ Save to Bronze │   │ COMPLETE    │
    │ (JSON file)    │   │ Ingestion   │
    └────────┬───────┘   └─────────────┘
             │
             ▼
    ┌──────────────────┐
    │ Update Checkpoint│
    │ • Save timestamp │
    │ • Save cursor    │
    └────────┬─────────┘
             │
             ▼
    ┌──────────────────┐
    │ Has More Pages?  │
    │ (next_after?)    │
    └────────┬─────────┘
             │
        ┌────┴────┐
        │          │
       YES        NO
        │          │
        ▼          ▼
   (Loop)     (End)
```

## Data Structures

### API Response (Search Mode)
```json
{
  "results": [
    {
      "id": "abc123",
      "createdAt": "2024-06-01T10:00:00.000Z",
      "updatedAt": "2024-06-22T19:00:00.000Z",
      "properties": {
        "email": "contact@example.com",
        "firstname": "John",
        "lastname": "Doe",
        "phone": "555-1234"
      }
    }
  ],
  "paging": {
    "next": {
      "after": "cursor_token_here",
      "link": "https://api.hubapi.com/..."
    }
  }
}
```

### Bronze Layer JSON File
```json
[
  {
    "id": "contact-123",
    "properties": {
      "email": "user@example.com",
      "firstname": "Jane",
      "lastname": "Smith",
      "phone": "555-5678"
    },
    "updatedAt": "2024-06-22T15:30:00.000Z"
  },
  {
    "id": "contact-124",
    "properties": {
      "email": "another@example.com",
      "firstname": "Bob",
      "lastname": "Johnson",
      "phone": "555-9999"
    },
    "updatedAt": "2024-06-22T16:45:00.000Z"
  }
]
```

### Checkpoint File
```json
{
  "last_after": null,
  "last_id": "contact-456",
  "last_updated_at": "2024-06-22T19:00:00.000Z",
  "last_run_time": "2024-06-22T19:31:38.123456"
}
```

## Error Handling Flow

```
┌─────────────────────┐
│  API Request        │
└──────────┬──────────┘
           │
           ▼
┌──────────────────────────────────┐
│ Did request succeed?             │
│ (response.ok)                    │
└──────┬─────────────────────┬─────┘
       │YES                  │NO
       │                     │
       ▼                     ▼
┌────────────────┐  ┌──────────────────────┐
│ Parse JSON     │  │ Log Error Details:   │
│ Extract Data   │  │ • Status code        │
└────────┬───────┘  │ • Response text      │
         │          │ • Request body       │
         │          │ • Request params     │
         │          └──────┬───────────────┘
         │                 │
         ▼                 ▼
    ┌─────────┐      ┌────────────┐
    │ SUCCESS │      │ EXCEPTION  │
    │ RETURN  │      │ RAISED     │
    │ DATA    │      │ (Stops)    │
    └─────────┘      └────────────┘
```

## Timing Architecture

```
Notebook Execution Timeline:

START
  │
  ├─ Config Load (1-2 sec)
  │
  ├─ CONTACTS Ingestion
  │  ├─ API Calls: 50 × (1-2 sec) = 50-100 sec
  │  ├─ File Write: 50 × (0.1 sec) = 5 sec
  │  └─ Subtotal: 55-105 sec
  │
  ├─ COMPANIES Ingestion
  │  ├─ API Calls: 50-100 × (1-2 sec) = 50-200 sec
  │  ├─ File Write: 50-100 × (0.1 sec) = 5-10 sec
  │  └─ Subtotal: 55-210 sec [⚠️ HIGH VARIANCE]
  │
  ├─ DEALS Ingestion
  │  ├─ API Calls: 20 × (1-2 sec) = 20-40 sec
  │  ├─ File Write: 20 × (0.1 sec) = 2 sec
  │  └─ Subtotal: 22-42 sec
  │
END (Total: 130-360 sec / 2-6 minutes)
    [⚠️ Company ingestion high variance due to API call count]
```

## Scalability Considerations

### Current Limitations
- **Sequential Processing**: Contacts → Companies → Deals (one at a time)
- **Batch Size**: Fixed at 100 records per API call
- **Memory**: Entire batch loaded in memory before JSON serialization
- **I/O**: Lakehouse file system I/O could be bottleneck

### Scalability Opportunities
1. **Parallel Object Ingestion**: Run contacts, companies, deals concurrently
2. **Batch Size Tuning**: Increase to 500-1000 for faster API consumption
3. **Streaming Writes**: Stream JSON instead of loading entire batch
4. **Distributed Processing**: Use PySpark parallelism for multiple objects
5. **API Rate Limit Awareness**: Monitor 429 responses and adjust concurrency

## Dependencies & External Systems

```
┌──────────────────────────┐
│  Azure Fabric/Synapse    │
│  • Notebook Runtime      │
│  • PySpark Kernel        │
│  • Lakehouse Storage     │
└──────────────────────────┘
         ↑
         │ (dependencies)
         │
┌──────────────────────────┐
│  Python Libraries        │
│  • requests              │
│  • urllib3               │
│  • json                  │
│  • datetime              │
│  • time                  │
│  • os                    │
└──────────────────────────┘
         ↑
         │ (imports)
         │
┌──────────────────────────┐
│  HubSpot API v3          │
│  • List Endpoint         │
│  • Search Endpoint       │
│  • Authentication        │
└──────────────────────────┘
```
