## Orchestration (Task Scheduling on Local)

This project runs the **Bronze ingestion pipeline** on a local Windows machine using **Windows Task Scheduler**.

The orchestration layer is implemented through a CLI entrypoint located at:
- `master_run/run.bat`

### What it provides
- **Scheduled executions** via Windows Task Scheduler  
- **CLI-based execution** using `master_run/run.bat`  
- **Centralized logging** (`/logs`)  
- **Exit code tracking** for failure detection (last run status stored by Task Scheduler)

### Folder
- `master_run/run.bat` : main execution entrypoint for the Bronze pipeline  
- `master_run/setup_venv.py` : local environment setup  
- `logs/` : runtime execution logs  

### Run manually
```bash
master_run/run.bat
