"""Pydantic models for script.yaml metadata configuration."""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ScheduleConfig(BaseModel):
    """Schedule configuration for scripts."""

    cron_schedule: str = Field(..., description="Cron expression for the schedule")
    timezone: str = Field(default="UTC", description="Timezone for the schedule")
    default_status: str = Field(
        default="RUNNING",
        description="Default status for the schedule (RUNNING or STOPPED)"
    )


class RetryPolicyConfig(BaseModel):
    """Retry policy configuration."""

    max_retries: int = Field(default=3, description="Maximum number of retries", ge=0, le=10)
    delay: int = Field(default=60, description="Initial delay in seconds", ge=0)
    backoff: str = Field(default="LINEAR", description="Backoff strategy (LINEAR or EXPONENTIAL)")
    jitter: Optional[str] = Field(default=None, description="Jitter strategy (FULL or PLUS_MINUS)")


class PrefectMappingConfig(BaseModel):
    """Prefect → Dagster mapping configuration."""

    enabled: bool = Field(default=False, description="Enable Prefect task mapping to Dagster ops")
    fallback_on_error: bool = Field(
        default=True,
        description="If parsing fails, fall back to running as subprocess"
    )
    mode: str = Field(default="graph_asset", description="Mapping mode (graph_asset is recommended)")


class AirflowMappingConfig(BaseModel):
    """Airflow → Dagster mapping configuration."""

    enabled: bool = Field(default=False, description="Enable Airflow task mapping to Dagster ops")
    dag_id: Optional[str] = Field(
        default=None,
        description="Specific DAG ID to map (if file contains multiple DAGs)"
    )
    fallback_on_error: bool = Field(
        default=True,
        description="If parsing fails, fall back to running as subprocess"
    )
    mode: str = Field(default="graph_asset", description="Mapping mode (graph_asset is recommended)")


class PartitionConfig(BaseModel):
    """Partition configuration for time-based partitioning."""

    parameter: str = Field(..., description="Name of the script parameter to use as partition key")
    schedule: str = Field(
        default="daily",
        description="Partition schedule: hourly, daily, weekly, monthly"
    )
    start_date: Optional[str] = Field(
        default=None,
        description="Start date for partitions (YYYY-MM-DD), defaults to 30 days ago"
    )
    timezone: str = Field(default="UTC", description="Timezone for partitions")
    date_format: str = Field(
        default="%Y-%m-%d",
        description="Date format string for passing to script (strftime format)"
    )


class DagFactoryConfig(BaseModel):
    """DAG Factory configuration for creating partitioned assets from multiple similar DAGs."""

    enabled: bool = Field(default=False, description="Enable DAG Factory → Partitioned Asset conversion")
    partition_key: str = Field(
        ...,
        description="The parameter that varies across DAGs (e.g., 'customer_id', 'region')"
    )
    partition_values: Optional[List[str]] = Field(
        default=None,
        description="Static list of partition values (e.g., ['customer_a', 'customer_b'])"
    )
    dynamic: bool = Field(
        default=False,
        description="Use dynamic partitions (values can be added/removed at runtime)"
    )
    template_dag: Optional[str] = Field(
        default=None,
        description="Path to template DAG file (if using template pattern)"
    )
    auto_detect: bool = Field(
        default=True,
        description="Auto-detect similar DAGs and suggest partitioning"
    )


class ScriptMetadata(BaseModel):
    """Complete script metadata configuration from script.yaml."""

    enabled: bool = Field(default=True, description="Whether this script is enabled")
    script_type: str = Field(
        default="python",
        description="Type of script: python, prefect, airflow, spark, dask"
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of the script"
    )
    group_name: str = Field(
        default="scripts",
        description="Dagster asset group name",
        alias="group"  # Accept 'group' in YAML
    )
    owners: List[str] = Field(default_factory=list, description="List of owner emails")
    tags: Dict[str, str] = Field(default_factory=dict, description="Custom tags")
    kinds: List[str] = Field(default_factory=list, description="Asset kinds for categorization and icons")

    # Dependencies
    depends_on: List[str] = Field(
        default_factory=list,
        description="List of script names this script depends on"
    )

    # Scheduling
    schedule: Optional[ScheduleConfig] = Field(
        default=None,
        description="Schedule configuration"
    )

    # Retry configuration
    retry_policy: Optional[RetryPolicyConfig] = Field(
        default=None,
        description="Retry policy configuration with backoff and jitter"
    )

    # Prefect mapping configuration
    prefect_mapping: Optional[PrefectMappingConfig] = Field(
        default=None,
        description="Prefect → Dagster mapping configuration"
    )

    # Airflow mapping configuration
    airflow_mapping: Optional[AirflowMappingConfig] = Field(
        default=None,
        description="Airflow → Dagster mapping configuration"
    )

    # Partition configuration
    partition: Optional[PartitionConfig] = Field(
        default=None,
        description="Partition configuration for time-based partitioning"
    )

    # DAG Factory configuration
    dag_factory: Optional[DagFactoryConfig] = Field(
        default=None,
        description="DAG Factory configuration for converting multiple similar DAGs to partitioned assets"
    )

    class Config:
        extra = "allow"  # Allow additional fields for extensibility
        populate_by_name = True  # Allow field name or alias
