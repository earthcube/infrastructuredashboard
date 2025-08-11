# Infrastructure Dashboard Implementation Plan

## Phase 1: Core Infrastructure Improvements

### 1.1 Code Refactoring & Cleanup
- **Remove debug logging** from production code (utils.py lines 7-13)
- **Fix authentication issues** with S3 client (utils.py lines 97-98: commented out credentials)
- **Standardize error handling** across all pages
- **Create configuration classes** to replace direct secret access
- **Add proper logging** instead of print statements

### 1.2 Infrastructure Page Enhancements
- **Tenant and source listing**: Read from MinIO `scheduler/configs/gleanerconfig.yaml`
- **Active sources display**: Parse `scheduler/configs/tenant.yaml` 
- **Add links to individual source reports**
- **Implement health check indicators** with proper status colors and timestamps

## Phase 2: Enhanced Monitoring Features

### 2.1 Portainer Integration Fixes
- **Resolve API version compatibility**: Complete transition from docker-py to requests
- **Implement proper authentication** for read-only API keys
- **Add contaner resource monitoring** (CPU, memory usage)
- **Create service dependency mapping**

### 2.2 Scheduler Improvements  
- **Fix timestamp calculations** (current TODO in 2_Scheduler.py:6)
- **Implement proper date filtering** for recent jobs
- **Add job duration metrics** and performance tracking
- **Create alert system** for failed jobs exceeding thresholds

### 2.3 Log Management Enhancement
- **Implement log search and filtering**
- **Add log content preview** without full download
- **Create log aggregation views** by service/time period
- **Add export functionality** for filtered logs

## Phase 3: Individual Source Statistics

### 3.1 Source Report System
- **Create report template system** 
- **Implement report generation** in `reports/{source}` structure
- **Add data visualization components** using Plotly/Altair
- **Create automated report scheduling**

### 3.2 Data Pipeline Monitoring
- **SPARQL query performance tracking**
- **Data ingestion rate monitoring** 
- **Quality metrics dashboard** per source
- **Historical trend analysis**

## Phase 4: Technical Infrastructure

### 4.1 Configuration Management
- **Create environment-specific configs**
- **Implement secrets validation**
- **Add configuration hot-reload**
- **Create deployment configurations**

### 4.2 Testing & Quality
- **Add unit tests** for utility functions
- **Create integration tests** for external services
- **Implement linting** (black, flake8, mypy)
- **Add pre-commit hooks**

### 4.3 Documentation
- **API documentation** for all utility functions
- **Deployment guide** for different environments
- **User manual** for dashboard features
- **Development setup** instructions

## Immediate Priority Items

1. **Fix S3 authentication** in utils.py
2. **Remove debug logging** from production
3. **Implement proper error handling** 
4. **Fix timestamp calculation** in scheduler
5. **Create configuration classes**
