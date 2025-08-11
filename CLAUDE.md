# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Streamlit-based infrastructure monitoring dashboard for the EarthCube/Geocodes infrastructure. The application monitors multiple services across different environments (dev/prod) including Docker containers via Portainer, Dagster scheduler jobs, S3/MinIO storage, and triplestores.

## Running the Application

```bash
# Install dependencies
pip install -e .

# Run the Streamlit application
streamlit run app.py
```

## Architecture

### Main Components

- `app.py`: Main Streamlit application entry point that defines navigation between pages
- `utils.py`: Core utility functions for connecting to external services (Portainer, Dagster, MinIO, triplestores)
- `apps/`: Directory containing individual dashboard pages

### Page Structure

1. **Infrastructure** (`apps/0.Infrastructure.py`): Enhanced infrastructure dashboard with:
   - Service health monitoring with detailed metrics and timestamps
   - Data sources configuration loaded from MinIO `gleanerconfig.yaml`
   - Active sources by tenant from `tenant.yaml`
   - Links to individual source reports (placeholder for future implementation)
2. **Gleaner Ingest In Progress** (`apps/1_Gleaner_Ingest_In_Progress.py`): Docker container monitoring via Portainer API
3. **Scheduler** (`apps/2_Scheduler.py`): Dagster job monitoring with GraphQL queries for job status (started, success, failed)
4. **Gleaner Logs** (`apps/3_Gleaner_Logs.py`): S3/MinIO log file browsing and filtering

### External Service Integration

- **Portainer**: Docker container management - uses custom requests implementation due to API version compatibility issues
- **Dagster**: Job scheduler monitoring via GraphQL API with templated queries for different job states
- **MinIO/S3**: Object storage for configuration and logs
- **Triplestores**: Graph database health monitoring

### Configuration

Uses Streamlit secrets management with server configurations prefixed with `GLEANER_`. Each server configuration includes:
- S3/MinIO endpoints and credentials
- Portainer API keys and URLs
- Graph server URLs
- Dagster GraphQL endpoints

Copy `secrets_toml.example.txt` to `.streamlit/secrets.toml` and configure with actual credentials.

### Known Issues

- Portainer API integration requires direct requests calls instead of docker-py due to version compatibility
- Read-only Portainer API keys have limitations requiring workarounds
- Some debugging logging is enabled globally in utils.py

## Code Patterns

- All pages import `utils` for service connections
- Server discovery via `utils.servers(st.secrets)` which filters secret keys with `GLEANER_` prefix
- GraphQL queries use string templates for dynamic prefix substitution
- Error handling displays service status with Streamlit success/error indicators