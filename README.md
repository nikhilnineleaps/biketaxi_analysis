# Bike Taxi Analytics ETL Pipeline

A comprehensive data analytics platform for bike taxi operations, featuring a complete ETL pipeline that processes operational data through Bronze → Silver → Gold layers with integrated data quality checks and business intelligence dashboards.

## Project Overview

This project implements a modern data architecture for analyzing bike taxi operations, including:
- **Users** and **Captains** (drivers) management
- **Rides** booking and completion tracking  
- **Payments** processing and discount analytics
- **Feedback** and rating systems
- **Automated reconciliation** and data quality monitoring

### Architecture
```
Google Sheets → Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated) → Dashboards
```

## Quick Start

### Prerequisites
- Python 3.8+
- PostgreSQL 12+
- Google Cloud Service Account (for Sheets API)
- Git

### 1. Clone Repository
```bash
git clone <repository-url>
cd biketaxi_analysis
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Environment Setup
Create `.env` file from template:
```bash
cp .env.example .env
```

Edit `.env` with your credentials:
```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=biketaxi_db
DB_USER=your_db_user
DB_PASS=your_db_password

# Google Sheets Configuration
GOOGLE_SHEETS_SPREADSHEET_ID=your_spreadsheet_id
SERVICE_ACCOUNT_FILE=config/credentials.json
```

### 4. Google Sheets Setup
1. Create Google Cloud Service Account
2. Download credentials JSON file
3. Save as `config/credentials.json`
4. Share your Google Sheet with the service account email

### 5. Run ETL Pipeline
```bash
python src/etl.py
```

## Project Structure

```
biketaxi_analysis/
├── bronze_inputs/              # Raw CSV data extracted from Google Sheets
│   ├── users.csv
│   ├── captains.csv
│   ├── rides.csv
│   ├── payments.csv
│   └── feedback.csv
├── config/                     # Configuration & credentials
│   ├── credentials.json        # Google Service Account (not in git)
│   └── credentials_example.json
├── src/                        # Core ETL pipeline
│   ├── etl.py                  # Main orchestrator
│   ├── extraction.py           # Google Sheets → Bronze layer
│   ├── transform_data.py       # Bronze → Silver/Audit layers
│   └── push_gold_to_sheets.py  # Gold → Google Sheets dashboards
├── transform/                  # Data cleaning modules
│   ├── clean_users.py          # User data validation
│   ├── clean_captains.py       # Captain data validation
│   ├── clean_rides.py          # Ride data validation
│   ├── clean_payments.py       # Payment data validation
│   └── clean_feedback.py       # Feedback data validation
├── load_data/                  # Gold layer aggregations
│   ├── users_aggregate.py      # User KPIs & metrics
│   ├── captain_aggregate.py    # Captain performance metrics
│   └── dashboard.py            # Unified dashboard data
├── logs/                       # Pipeline execution logs
│   └── etl_log.txt
├── test/                       # Data quality reports
│   └── reconciliation_report.csv
├── .env.example               # Environment template
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## Data Flow & Processing

### 1. Bronze Layer (Raw Data)
- **Source**: Google Sheets with 5 tabs (users, captains, rides, payments, feedback)
- **Process**: Direct extraction → CSV files → PostgreSQL bronze schema
- **Purpose**: Preserve original data exactly as received

### 2. Silver Layer (Cleaned Data)
- **Process**: Data validation, cleaning, and normalization
- **Features**:
  - ID format validation (user00001, CP00001)
  - Date parsing with multiple format support
  - Missing value imputation (median for numeric, defaults for categorical)
  - Referential integrity enforcement
  - Duplicate removal
- **Invalid Data**: Moved to **Audit schema** with rejection reasons

### 3. Gold Layer (Business Metrics)
- **User Aggregates**: Signup trends, ride frequency, payment behavior
- **Captain Aggregates**: Performance metrics, earnings, ratings, activity status
- **Dashboard**: Unified view combining all entities for BI tools

## Key Features

### Data Quality & Validation
- **Comprehensive validation rules** for each data entity
- **Audit trail** for all rejected/invalid records
- **Reconciliation reports** comparing Silver vs Gold metrics
- **Data consistency checks** across related tables

### Monitoring & Observability  
- **Centralized logging** with timestamps and log levels
- **ETL pipeline status tracking** with success/failure indicators
- **Data lineage documentation** through transformation logs
- **Automated reconciliation** with 18-point validation matrix

### Business Intelligence
- **Real-time dashboards** via Google Sheets integration
- **Captain performance analytics** (earnings, ratings, activity)
- **User behavior analysis** (ride patterns, payment preferences)
- **Operational metrics** (completion rates, feedback analysis)

## Usage Examples

### Run Full ETL Pipeline
```bash
python src/etl.py
```

### Run Individual Components
```bash
# Extract data only
python -c "from src.extraction import export_sheets_to_csv; export_sheets_to_csv()"

# Transform data only  
python src/transform_data.py

# Generate aggregates only
python load_data/users_aggregate.py
```

### Check Pipeline Status
```bash
tail -f logs/etl_log.txt
```

## Database Schema

### Bronze Schema (Raw)
All tables store data as TEXT to preserve original format:
- `bronze.users`, `bronze.captains`, `bronze.rides`, `bronze.payments`, `bronze.feedback`

### Silver Schema (Cleaned)
Properly typed tables with constraints:
- `silver.users` (user_id PK, signup_date DATE, age INTEGER)
- `silver.captains` (captain_id PK, rating DECIMAL, experience_years INTEGER)  
- `silver.rides` (ride_id PK, user_id FK, captain_id FK, ride_date DATE)
- `silver.payments` (payment_id PK, ride_id FK, fare DECIMAL)
- `silver.feedback` (feedback_id PK, ride_id FK, user_rating INTEGER)

### Gold Schema (Aggregated)
Business-ready analytical tables:
- `gold.user_aggregate` - User KPIs and behavior metrics
- `gold.captain_aggregate` - Captain performance and earnings
- `gold.dashboard` - Denormalized data for BI tools

### Audit Schema (Invalid Data)
Mirror of Silver schema + rejection metadata:
- All rejected records with `reason` and `run_ts` fields

## Data Quality Checks

### User Validation
- User ID format: `user00001` pattern
- Valid signup dates (YYYY-MM-DD, MM/DD/YYYY, DD/MM/YYYY)
- Age range validation with median imputation
- Duplicate removal

### Captain Validation  
- Captain ID format: `CP00001` pattern
- Name presence validation
- Rating range (0-5) with median imputation
- Experience years validation

### Ride Validation
- Valid user/captain references
- Ride date consistency
- Distance/duration numeric validation
- Status standardization

### Cross-Entity Validation
- Rides must have valid user/captain references
- Payments must reference existing rides
- Feedback must reference existing rides
- No rides before user signup date

## Monitoring & Alerts

### Log Levels
- **INFO**: Normal pipeline progress
- **ERROR**: Failures with full stack traces
- **SUCCESS**: Completion confirmations

### Reconciliation Checks
The pipeline validates 18 key metrics:
- Row counts across Bronze/Silver/Gold layers
- Aggregate calculations (sums, averages, counts)
- Referential integrity between related tables

Pipeline only pushes to dashboards if **ALL** reconciliation checks pass.

## Development

### Adding New Data Sources
1. Add sheet name to `SHEETS` dict in `src/extraction.py`
2. Create table schema in `create_table_queries`
3. Add cleaning function in `transform/clean_[entity].py`
4. Update `src/transform_data.py` to include new entity

### Extending Aggregations
1. Create new module in `load_data/`
2. Follow pattern: `create_or_replace_[entity]_aggregate()`
3. Add reconciliation function
4. Update `src/etl.py` to include new aggregate

### Custom Validation Rules
1. Modify appropriate `transform/clean_[entity].py` file
2. Add rejection reasons to `df_rejects`
3. Update tests and documentation

## Security

- Service account credentials not committed to git
- Environment variables for sensitive configuration  
- Database connection using connection pooling
- Input validation to prevent SQL injection

## Troubleshooting

### Common Issues

**"Missing SPREADSHEET_ID or SERVICE_ACCOUNT_FILE"**
- Check `.env` file configuration
- Verify `config/credentials.json` exists

**"Failed to connect to database"**
- Verify PostgreSQL is running
- Check database credentials in `.env`
- Ensure database exists

**"Reconciliation validation failed"**
- Check `test/reconciliation_report.csv` for details
- Review `logs/etl_log.txt` for transformation errors
- Invalid data moved to audit schema for investigation

**"Google Sheets API quota exceeded"**
- Implement exponential backoff (future enhancement)
- Consider caching extracted data

### Debug Mode
Add verbose logging by setting environment variable:
```bash
export ETL_DEBUG=true
python src/etl.py
```

