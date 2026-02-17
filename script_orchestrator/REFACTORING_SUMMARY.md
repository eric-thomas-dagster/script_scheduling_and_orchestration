# Parser Refactoring & Airflow Schedule/Retry Support

## Summary

Successfully refactored the script orchestrator component by extracting Prefect and Airflow parsing logic into separate, maintainable modules. Added automatic schedule and retry policy extraction from Airflow DAG decorators.

## Changes Made

### 1. Created Parser Modules (`script_orchestrator/components/parsers/`)

**`base_parser.py`** - Shared utilities:
- `has_decorator()` - Check for decorators on functions
- `has_return_statement()` - Check if function returns a value
- `extract_function_parameters()` - Extract function signature parameters
- `extract_decorator_kwargs()` - Extract keyword args from decorators

**`prefect_parser.py`** - Prefect-specific parsing:
- `parse_flow()` - Parse Prefect flows and tasks
- `create_graph_asset()` - Create Dagster graph assets from flows
- Task retry config extraction
- Task dependency analysis

**`airflow_parser.py`** - Airflow-specific parsing with enhanced features:
- `parse_dag()` - Parse Airflow DAGs and tasks
- `create_graph_asset()` - Create Dagster graph assets from DAGs
- **NEW**: `_extract_dag_config()` - Extract schedule, retries, retry_delay from @dag decorator
- **NEW**: `_extract_timedelta_seconds()` - Convert timedelta to seconds for retry delays
- **NEW**: Schedule extraction from `schedule` parameter
- **NEW**: Retry policy extraction from `default_args` or DAG-level params

### 2. Refactored Component (`script_github_component.py`)

**Removed ~400 lines** of parsing logic, delegating to parser modules:
- Removed `_parse_prefect_flow` implementation (now delegates to `PrefectParser`)
- Removed `_parse_airflow_dag` implementation (now delegates to `AirflowParser`)
- Removed `_create_prefect_flow_graph_asset` implementation (now delegates to parser)
- Removed `_create_airflow_dag_graph_asset` implementation (now delegates to parser)
- Removed helper methods: `_has_decorator`, `_has_return_statement`, `_extract_task_retry_config`, `_extract_flow_parameters`, `_extract_task_calls`, `_extract_airflow_dag_params`, `_extract_airflow_param_info`, `_extract_airflow_task_calls`

**Added**:
- Parser initialization in `__init__` method
- Automatic schedule extraction for Airflow DAGs
- Automatic retry policy extraction for Airflow DAGs
- Smart fallback: YAML config takes precedence over DAG-level settings

### 3. Enhanced Airflow Support

**Schedule Extraction**:
- Reads `schedule` or `schedule_interval` from @dag decorator
- Converts to Dagster `ScheduleConfig` with cron expression
- Only applies if not already defined in YAML config
- Supports cron expressions: `"0 9 * * *"` (daily at 9am)

**Retry Policy Extraction**:
- Reads `retries` from @dag decorator or `default_args`
- Reads `retry_delay` from `default_args` (timedelta)
- Converts timedelta to seconds for Dagster `RetryPolicy`
- Only applies if not already defined in YAML config

**Example**:
```python
@dag(
    dag_id="simple_sequential_pipeline",
    schedule="0 9 * * *",  # Extracted as schedule
    default_args={
        "retries": 3,        # Extracted as retry policy
        "retry_delay": timedelta(minutes=5),
    },
)
```

### 4. Updated Example (`simple_sequential.py`)

Added schedule and retry policy to demonstrate auto-extraction:
```python
schedule="0 9 * * *",  # Daily at 9am
default_args={
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
},
```

## Results

### Before Refactoring
- `script_github_component.py`: ~1700 lines
- Parsing logic mixed with component logic
- 4 schedules total

### After Refactoring
- `script_github_component.py`: ~1300 lines (**-400 lines**)
- `parsers/` module: 3 new files with ~700 lines of focused parsing logic
- **5 schedules total** (added 1 from Airflow DAG auto-extraction)
- Clean separation of concerns
- Easier to maintain and extend

### Feature Parity
✅ All existing functionality preserved
✅ All tests pass
✅ 23 assets loaded successfully
✅ 3 Airflow assets (1 graph, 2 subprocess)
✅ 9 Prefect assets
✅ **NEW**: Automatic schedule extraction from Airflow DAGs
✅ **NEW**: Automatic retry policy extraction from Airflow DAGs

## Benefits

1. **Maintainability**: Parsing logic isolated in dedicated modules
2. **Testability**: Parsers can be unit tested independently
3. **Extensibility**: Easy to add new workflow system parsers (Temporal, etc.)
4. **Clarity**: Component focuses on orchestration, parsers focus on parsing
5. **Feature Rich**: Airflow DAGs now automatically get schedules and retry policies

## Testing

```bash
# Verify component loads
uv run python -c "from script_orchestrator.components.script_github_component import ScriptGithubComponent; print('✓')"

# Verify definitions load with schedules
uv run python -c "from script_orchestrator.definitions import defs; print(f'{len(list(defs.resolve_all_asset_specs()))} assets')"
# Output: 23 assets
# Logs: "Created 20 assets and 5 schedules" ✓

# Start Dagster dev server
uv run dg dev
# View at http://localhost:3000
```

## Architecture

```
script_github_component.py
├── parsers/
│   ├── __init__.py
│   ├── base_parser.py      (shared utilities)
│   ├── prefect_parser.py   (Prefect-specific)
│   └── airflow_parser.py   (Airflow-specific + schedule/retry)
```

## Next Steps

1. ✅ Refactor parsers into modules
2. ✅ Add Airflow schedule extraction
3. ✅ Add Airflow retry policy extraction
4. ⏭️ Add unit tests for parsers
5. ⏭️ Consider adding Temporal parser
6. ⏭️ Add support for more Airflow features (sensors, operators)
