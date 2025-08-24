# Acrisure Migrator
## Overview
This repository contains a suite of tools designed to facilitate seamless data and metadata migration between HubSpot portals. The toolkit includes a primary data migrator along with several auxiliary tools for property management, data verification, and system cleanup.

## git usage
See full github usage guide [here](https://github.com/Aptitude-8/a8-knowledge/blob/main/processes/tech/github-standards.md)

**Prefix Categories**

```
feat/ - New features
fix/ - Bug fixes
chore/ - Maintenance tasks
docs/ - Documentation updates
style/ - Code style/formatting changes
refactor/ - Code refactoring
test/ - Test additions/updates
```
Examples
```
feat/add-login-form/{cu-taskid}
fix/resolve-api-timeout/{cu-taskid}
chore/update-dependencies/{cu-taskid}
docs/update-readme/{cu-taskid}
refactor/optimize-queries/{cu-taskid}
```
Format
- Use lowercase letters
- Separate words with hyphens
- Keep names concise but descriptive
- Include ticket number if applicable (for CU integration): `feat/login-form-TICK-123`

## Core Features

### Data Migration
- **Extract**
  - Full data extraction capabilities
  - Incremental extraction of new/updated records
  - Selective extraction with filtering options (object type, date range, properties)
  - CSV import support for offline data migration

- **Transform**
  - Dynamic property mapping between source and target
  - Customizable transformation rules for data formatting and enhancement

- **Load**
  - Complete data loading functionality
  - Incremental loading support
  - Selective loading with filters
  - ID mapping system for cross-portal record alignment

### Relationship Management
- Preservation of associations between contacts, companies, deals, and engagements
- Automated duplicate detection and merging based on configurable rules

### Verification & Monitoring
- One-to-one data comparison between source and target
- Comprehensive error logging with retry mechanisms
- Detailed run reports with statistics and error summaries

### Auxiliary Tools
- **Property Management**
  - Property copying between portals
  - Association template copying
  - CSV-based property creation

- **System Maintenance**
  - Complete target portal reset capability
  - Selective data deletion by object type or date range

### Usage
The toolkit operates via command-line interface, with configurations specified through config files for flexible and repeatable operations.
