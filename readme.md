# NordHealth Challenge - Data Pipeline Documentation

## Overview
This documentation provides a comprehensive step-by-step walkthrough of the data pipeline from raw data ingestion to final presentation dashboards.

## Architecture
The pipeline follows a layered architecture pattern:
- **Layer A**: Raw Data (`a_raw_data/`)
- **Layer B**: Staging (`b_staging/`)
- **Layer C**: Features (`c_features/`)
- **Layer D**: Presentation (`d_presentation/`)

Each layer has a specific responsibility and outputs data for the next layer to consume.

### File Structure
```
nordhealth_challenge_pedro_miranda/
├── a_raw_data/
│   └── Dummy dataset - Sheet1.csv
├── b_staging/
│   ├── f_staging_events.py
│   └── data_output/
├── c_features/
│   ├── f_funnel_data.py
│   ├── f_churn_data.py
│   ├── f_inconsistencies.py
│   └── data_output/
├── d_presentation/
│   ├── p_funnel.py
│   ├── p_churn.py
│   ├── p_inconsistencies.py
│   └── dashboards/
└── README.md (this documentation)
```

## Layer A: Raw Data (`a_raw_data/`)

### Purpose
Contains the original, unprocessed data files as received from source systems.

### Files
- **`Dummy dataset - Sheet1.csv`**: Original customer event data
- **`Dummy dataset - Sheet1.csvZone.Identifier`**: Windows zone identifier file

### Data Structure
The raw CSV contains client events with the following columns:
- `record_id`: Unique identifier for each event record
- `client_id`: Customer identifier
- `event_type`: Type of event (applied, signed, churned, docs_submitted, rejected)
- `event_date`: When the event occurred (YYYY-MM-DD format)
- `plan`: Customer plan type (Premium, Basic, Pro, Unknown)
- `region`: Geographic region (US, CA, UK, BR, IT)
- `marketing_channel`: How customer was acquired (Email, Organic Search, Paid Ads, Referral)
- `sales_rep_id`: Sales representative identifier (may be -1 for missing)
- `source_system`: System that generated the event (internal_form, web_api, manual_upload)

### Data Quality Issues (Identified)
- Some events have `plan = 'Unknown'`
- Missing sales rep assignments (`sales_rep_id = -1`)
- Chronological inconsistencies (events out of order)
- Very few `docs_submitted` events relative to applications

## Layer B: Staging (`b_staging/`)

### Purpose
Data cleaning, validation, and preparation for analysis. Creates a clean, standardized dataset.

### Processing Script
**`f_staging_events.py`** - Main staging processor

### Key Operations

#### 1. Data Loading & Cleaning
```python
class StagingEventsProcessor:
    def load_raw_data(self):
        # Load CSV with robust path handling
        # Convert dates to datetime format
        # Standardize column names (lowercase, underscores)
```

#### 2. Schema Validation
- Validates required columns exist
- Checks data types
- Ensures date columns are properly formatted
- Validates client_id is numeric

#### 3. Data Enhancement
- **Event Ranking**: Adds `event_rank` using SQL window functions
  ```sql
  ROW_NUMBER() OVER (PARTITION BY client_id, event_type ORDER BY event_date ASC)
  ```
- **Missing Value Handling**: Strategic filling of null values where appropriate

#### 4. SQL-based Transformation
Uses in-memory SQLite database for complex transformations:
- Efficient window function operations
- Consistent ranking logic
- Preparation for downstream analysis

### Output
**`b_staging/data_output/f_staging_events.csv`**
- Clean, validated event data
- Added event ranking
- Standardized format ready for feature engineering

## Layer C: Features (`c_features/`)

### Purpose
Feature engineering and business logic implementation. Creates analysis-ready datasets for specific use cases.

---

### Funnel Analysis (`f_funnel_data.py`)

#### Processing Logic
```python
class FunnelDataProcessor:
    def create_funnel_analysis(self):
        # Aggregates client journey stages
        # Tracks: applied_date, docs_submitted_date, rejected_date, signed_date, churned_date
        
    def analyze_funnel_metrics(self):
        # Calculates conversion rates between stages
        # Computes active clients, conversion percentages
```

#### Key Metrics Calculated
- **Applied Clients**: Total who started the process
- **Docs Submitted Rate**: % who submitted documentation
- **Rejection Rate**: % who were rejected
- **Conversion Rate**: % who successfully signed
- **Churn Rate**: % who churned after signing
- **Active Clients**: Currently in the pipeline

#### Output
**`c_features/data_output/f_funnel_data.csv`**
- Client-level funnel progression
- Key dates for each funnel stage
- Ready for funnel visualization

**`c_features/data_output/f_funnel_metrics.csv`**
- Comprehensive funnel performance metrics
- Aggregated statistics and conversion rates

---

### Churn Analysis (`f_churn_data.py`)

#### Processing Logic
```python
class ChurnDataProcessor:
    def create_churn_analysis(self):
        # Calculates days since last activity
        # Determines churn status and risk levels
        # Adds last event type and churn flag
```

#### Key Features Created
- **Days Since Last Event**: Inactivity measurement
- **Days Since Signed**: Post-conversion activity tracking
- **Risk Categorization**: High/Medium/Low risk based on inactivity
- **Churn Flag**: `is_churned` binary indicator
- **Last Event Type**: Most recent activity type

#### Risk Categories
- **Already Churned**: `is_churned = 1`
- **High Risk**: >60 days inactive (active clients)
- **Medium Risk**: 31-60 days inactive
- **Low Risk**: ≤30 days inactive
- **Unknown**: Missing activity data

#### Output
**`c_features/data_output/f_churn_data.csv`**
- Client risk assessment
- Churn status and timing
- Ready for retention analysis

---

### Inconsistencies Analysis (`f_inconsistencies.py`)

#### Processing Logic
```python
class InconsistenciesProcessor:
    def analyze_unknown_values(self):
        # Identifies records with missing/unknown field values
        
    def analyze_event_sequence_violations(self):
        # Detects chronological impossibilities
        
    def analyze_docs_submitted_pattern(self):
        # Investigates document submission gaps
        
    def analyze_multiple_applications(self):
        # Identifies clients with multiple application events
```

#### Four Key Scenarios Analyzed
1. **Unknown Values**: `plan='Unknown'`, `sales_rep_id=-1`
2. **Sequence Violations**: Events in wrong chronological order
3. **Document Submission Gap**: Why only 1 docs_submitted event
4. **Multiple Applications**: Clients applying multiple times

#### Output Files
- **`f_inconsistencies.csv`**: Main inconsistencies analysis
- **`f_inconsistencies_client_details.csv`**: Detailed client event data
- **`f_event_distribution_analysis.csv`**: Event type distribution statistics

## Layer D: Presentation (`d_presentation/`)

### Purpose
Interactive dashboards and visualizations for business insights. Converts analysis into actionable intelligence.

---

### Funnel Dashboard (`p_funnel.py`)

#### Dashboard Structure (4x2 Grid + Full-width Events Table)
```
[Summary Statistics]              [Funnel Conversion Flow]
[Days to Sign Distribution]       [Time Between Stages]
[Timeline Analysis]               [Stage Comparison]
[--- Detailed Client Events Table (Full Width) ---]
```

#### Key Visualizations
- **Conversion Funnel**: Visual flow from Applied → Docs → Signed → Churned
- **Time Analysis**: Days to sign, days to churn distributions
- **Events Table**: Client-level journey details with dates
- **Stage Metrics**: Conversion rates between each stage

#### Business Insights
- Identifies bottlenecks in the customer journey
- Shows time-to-conversion patterns
- Highlights clients progressing through each stage
- Reveals missing documentation step (only 1 client submitted docs)

#### Output
**`d_presentation/dashboards/funnel_analysis_dashboard.html`**

---

### Churn Dashboard (`p_churn.py`)

#### Dashboard Structure (4x2 Grid)
```
[Summary Statistics]              [Risk Distribution (Active)]
[Days Since Last Event (Active)]  [Churned vs At Risk]
[Timeline Analysis]               [Risk by Segment]
[--- Detailed Client Events Table (Full Width) ---]
```

#### Key Visualizations
- **Risk Distribution**: Pie chart of active clients by risk level
- **Activity Analysis**: Histogram of days since last event
- **Churn Events Table**: Detailed client progression with risk categories
- **Separation Logic**: Clear distinction between churned vs at-risk clients

#### Business Logic
- **Excludes churned clients** from risk analysis
- **Simple messaging**: "3 clients churned, 7 in risk of churn"
- **Color-coded table**: Risk levels with visual indicators
- **Actionable insights**: Focus on clients needing immediate attention

#### Output
**`d_presentation/dashboards/churn_analysis_dashboard.html`**

---

### Inconsistencies Dashboard (`p_inconsistencies.py`)

#### Dashboard Structure (4x2 Grid with Top Events Table)
```
[--- Problematic Events Details (Full Width) ---]
[Summary: Four Scenarios]         [Scenario 1: Unknown Values]
[Scenario 2: Date Violations]     [Scenario 3: Document Patterns]  
[Scenario 4: Multiple Apps]       [Event Distribution Context]
```

#### Four Key Scenarios
1. **Unknown Values**: Bar chart of missing field types
2. **Sequence Violations**: Timeline showing chronological errors
3. **Document Patterns**: Pie chart of submission vs non-submission
4. **Multiple Applications**: Scatter plot of application frequency

#### Events Table Features
- **Color-coded rows** by issue type
- **Detailed event information** with dates and values
- **Immediate visibility** into problematic records
- **Action-oriented display** for data quality teams

#### Output
**`d_presentation/dashboards/inconsistencies_analysis_dashboard.html`**

---

### Technical Design Principles

#### Interactive HTML Dashboards
- **Plotly-based**: Interactive charts with zoom, hover, pan
- **Responsive design**: Adapts to different screen sizes
- **Color consistency**: Professional color schemes across dashboards
- **Performance optimized**: Efficient data loading and rendering

#### User Experience
- **Business-focused insights**: Key findings prominently displayed
- **Actionable information**: Clear next steps for each issue type
- **Drill-down capability**: Summary → Details → Specific records
- **Export-friendly**: HTML format for sharing and embedding

## Data Flow Summary

### End-to-End Pipeline

```
a_raw_data/
├── Dummy dataset - Sheet1.csv (28 events, 9 clients)
└── Raw customer event data
            ↓
b_staging/
├── f_staging_events.py (StagingEventsProcessor)
├── Data cleaning, validation, event ranking
└── → f_staging_events.csv (clean, standardized)
            ↓
c_features/ (Parallel Processing)
├── f_funnel_data.py → f_funnel_data.csv (funnel metrics)
├── f_churn_data.py → f_churn_data.csv (risk analysis)
└── f_inconsistencies.py → 3 analysis files
            ↓
d_presentation/
├── p_funnel.py → funnel_analysis_dashboard.html
├── p_churn.py → churn_analysis_dashboard.html
└── p_inconsistencies.py → inconsistencies_analysis_dashboard.html
```

### Key Data Transformations

#### Raw → Staging
- **Input**: 28 raw events across 9 clients
- **Transformation**: Clean, validate, add event ranking
- **Output**: Standardized events ready for analysis

#### Staging → Features
- **Funnel**: Client journey aggregation (applied → docs → signed → churned)
- **Churn**: Risk assessment based on inactivity (days since last event)
- **Inconsistencies**: Data quality issues identification (4 key scenarios)

#### Features → Presentation
- **Interactive Dashboards**: HTML with Plotly visualizations
- **Business Insights**: Actionable intelligence for different stakeholders
- **Detailed Tables**: Drill-down capability for specific records

### Business Value Delivered

#### For Sales Operations
- **Funnel Analysis**: Identify conversion bottlenecks
- **Churn Prediction**: Proactive client retention (7 at-risk clients identified)
- **Lead Management**: 3 clients with multiple applications need attention

#### For Data Quality Team
- **Inconsistencies Dashboard**: 5 records with unknown values
- **Process Gaps**: Document submission step appears optional/missing
- **System Issues**: 1 client signed before applying (data entry error)

#### For Management
- **KPI Tracking**: Clear conversion rates and churn metrics
- **Resource Allocation**: Focus on high-risk clients and process improvements
- **Decision Support**: Data-driven insights for business strategy

## How to Run the Pipeline

### Installation

#### Install Dependencies
```bash
pip install -r requirements.txt
```

### Sequential Execution

#### 1. Staging Layer
```bash
cd b_staging
python f_staging_events.py
```
**Output**: `b_staging/data_output/f_staging_events.csv`

#### 2. Features Layer (Run all in parallel)
```bash
cd c_features
python f_funnel_data.py      # Creates funnel analysis
python f_churn_data.py       # Creates churn analysis  
python f_inconsistencies.py # Creates inconsistencies analysis
```
**Outputs**: 
- `c_features/data_output/f_funnel_data.csv`
- `c_features/data_output/f_funnel_metrics.csv`
- `c_features/data_output/f_churn_data.csv`
- `c_features/data_output/f_inconsistencies.csv`
- `c_features/data_output/f_inconsistencies_client_details.csv`
- `c_features/data_output/f_event_distribution_analysis.csv`

#### 3. Presentation Layer (Run all in parallel)
```bash
cd d_presentation
python p_funnel.py           # Creates funnel dashboard
python p_churn.py            # Creates churn dashboard
python p_inconsistencies.py # Creates inconsistencies dashboard
```
**Outputs**:
- `d_presentation/dashboards/funnel_analysis_dashboard.html`
- `d_presentation/dashboards/churn_analysis_dashboard.html`
- `d_presentation/dashboards/inconsistencies_analysis_dashboard.html`

### Dependencies
- **Python 3.8+**
- **Required packages**: `pandas>=2.0.0`, `plotly>=5.15.0`, `numpy>=1.24.0`
- **Data source**: `a_raw_data/Dummy dataset - Sheet1.csv`

## Key Findings & Insights

### Data Quality Issues Identified

#### Critical Issues (Require Immediate Attention)
1. **Sequence Violation**: Client 1009 signed (2023-01-28) before applying (2023-01-30)
2. **Missing Sales Attribution**: 2 clients have `sales_rep_id = -1`
3. **Unknown Plans**: 3 events have `plan = 'Unknown'`
4. **Document Process Gap**: Only 1 out of 9 clients submitted documents

#### Business Process Insights
- **Document Submission Optional**: 6 clients signed without submitting docs
- **Multiple Applications**: 3 clients applied multiple times (possible UX issue)
- **Conversion Rate**: 78% of applicants eventually sign (7 out of 9)
- **Churn Rate**: 44% of signed clients eventually churn (4 out of 9)

### Client Risk Assessment

#### Churned Clients (4)
- Client 1002, 1004, 1007, 1009

#### At Risk Clients (3) 
- Clients with >60 days inactivity but not churned
- Immediate intervention recommended

#### Active Clients (2)
- Clients with recent activity (<30 days)

### Funnel Analysis Results

#### Conversion Stages
- **Applied**: 9 clients
- **Docs Submitted**: 1 client (11% submission rate)
- **Signed**: 7 clients (78% conversion rate)
- **Churned**: 4 clients (44% of signed clients)
- **Currently Active**: 3 clients (33% retention rate)

#### Time to Conversion
- **Average Days to Sign**: Varies by client
- **Fastest Conversion**: Client 1001 (2 days)
- **Document Process**: When used, adds 2-3 days to journey

### Recommendations

#### For Sales Operations
1. **Immediate Action**: Contact 3 at-risk clients for retention
2. **Process Review**: Investigate why Client 1009 has sequence violation
3. **Lead Management**: Review multiple application patterns for UX improvements

#### For Data Quality Team
1. **Sales Rep Assignment**: Fix missing assignments (`sales_rep_id = -1`)
2. **Plan Classification**: Resolve 'Unknown' plan values
3. **Data Entry Training**: Prevent sequence violations in source systems

#### For Product Team
1. **Document Process**: Clarify if document submission is required or optional
2. **User Experience**: Reduce multiple applications through better UI/process
3. **Retention Strategy**: Focus on post-signature engagement to reduce churn

#### For Management
1. **KPI Monitoring**: Track 78% conversion rate and 44% churn rate trends
2. **Resource Allocation**: Prioritize retention over acquisition given high churn
3. **Process Optimization**: Document submission step needs clarification/improvement
