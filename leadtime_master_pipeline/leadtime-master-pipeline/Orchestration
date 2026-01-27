## Orchestration (Task Scheduling on Local)

This project runs the **Bronze ingestion pipeline** on a local Windows machine using **Windows Task Scheduler** as the orchestration mechanism.

### What it provides
- **Scheduled executions** (daily / hourly / every N minutes)
- **CLI-based execution** via scripts (`run_bronze.bat` / `run_bronze.ps1`)
- **Centralized logging** (`/logs`)
- **Exit code tracking** for failure detection (Task Scheduler stores last run result)

### Folder Structure
- `orchestration/run/` : executable entrypoints for pipeline runs  
- `orchestration/windows-task-scheduler/` : task definitions and setup scripts  
- `logs/` : runtime execution logs  

### Run manually
```bash
orchestration/run/run_bronze.bat

