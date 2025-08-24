# HubSpot Deleter

A Python package for managing HubSpot records, providing tools for seeding test data and deleting records.

[Video Overview ](https://drive.google.com/file/d/1WQnLwBdpAq1_6_NtlonQgAVf70Ww21lo/view?usp=sharing)

## Installation

1. Install using `uv`:
```bash
uv pip install -e .
```

2. Set up your HubSpot API token in a `.env` file:
```bash
HUBSPOT_TOKEN=your_hubspot_api_token_here
```

## Commands

### Seeding Data

The seeder provides commands to create test records in your HubSpot portal. Mainly this was used for testing the deleter which is the main component...

#### Create Contacts
Creates contacts with just famke email, no other properties
```bash
# Create multiple contacts
seeder seed-contacts --count 50
```

#### Create Companies
Creates companies with just name, no other properties
```bash
# Create multiple companies
seeder seed-companies --count 25
```

#### Create Deals
Creates deals with just name, stage and pipeline, no other properties. Stage and pipeline should be modified
```bash
# Create multiple deals
seeder seed-deals --count 10
```

### Deleting Records

The deleter provides various commands to clean up records in your HubSpot portal.

#### Delete All Records
```bash
# Delete all records
deleter delete-all
```

#### Delete Specific Object Types
```bash
# Delete all contacts
deleter delete-objects contacts

# Delete multiple object types
deleter delete-objects contacts companies deals
```

#### Delete by Date
```bash
# Delete records created after a specific date/time (UTC)
deleter delete-by-date --created-after "2024-03-01T00:00:00+0000"

# Delete records modified after a specific date/time (Eastern Time)
deleter delete-by-date --modified-after "2024-03-01T00:00:00-0400"  # EDT (Mar-Nov)
deleter delete-by-date --modified-after "2024-12-01T00:00:00-0500"  # EST (Nov-Mar)

# Delete specific object types by date
deleter delete-by-date --created-after "2024-03-01T00:00:00+0000" --object-types contacts --object-types companies

# Delete all object types by date
deleter delete-by-date --created-after "2024-03-01T00:00:00+0000" --object-types all
```

#### Delete from CSV
```bash
# Delete records listed in a CSV file
deleter delete-from-csv contacts my_contacts.csv

# Specify a custom ID column
deleter delete-from-csv companies companies.csv --id-column company_id
```

#### Delete by Property
```bash
# Delete all records that have a specific property
deleter delete-by-property contacts custom_property_name
```


## Supported Object Types

The following HubSpot object types are supported:
- contacts
- companies
- deals
- tickets
- notes
- emails
- tasks
- meetings
- calls

## Notes

### Timezone Handling
- All dates must include timezone information
- For Eastern Time:
  - Use `-0400` during EDT (March-November)
  - Use `-0500` during EST (November-March)
- UTC (`+0000`) can be used to avoid timezone complications

### Batch Processing
- Records are processed in batches of 100 for optimal performance
- The HubSpot Search API has a limit of 10,000 records per search
- Large deletions are automatically handled through pagination and multiple searches

### Error Handling
- All operations are logged for debugging purposes
- Error messages are displayed in the console

## Requirements
- Python 3.7+
- HubSpot API token with appropriate permissions
- `uv` package installer