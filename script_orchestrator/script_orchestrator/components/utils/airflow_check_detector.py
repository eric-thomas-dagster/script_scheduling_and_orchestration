"""Detect and parse Airflow check operators."""

import logging
import re
from typing import Any, Dict, List, Optional

from dagster import AssetCheckResult, AssetCheckSeverity

logger = logging.getLogger(__name__)


class AirflowCheckDetector:
    """Detect Airflow check operators and parse their results.

    Supports:
    - SQLColumnCheckOperator - Column-level SQL checks
    - SQLTableCheckOperator - Table-level SQL checks
    - SQLCheckOperator - Custom SQL checks
    - PythonCheckOperator - Custom Python checks (basic support)
    """

    # Map operator types to check categories
    CHECK_OPERATORS = {
        'sql_column_check': 'sql_column',
        'sql_table_check': 'sql_table',
        'sql_check': 'sql_custom',
        'python_check': 'python',
    }

    @staticmethod
    def detect_check_operators(dag_info: Dict) -> List[Dict[str, Any]]:
        """Detect check operators in an Airflow DAG.

        Args:
            dag_info: Parsed DAG information containing tasks

        Returns:
            List of detected check definitions with:
            - task_id: Task identifier
            - check_type: Type of check (sql_column, sql_table, etc.)
            - table: Table being checked (for SQL checks)
            - checks: Check configuration
        """
        detected_checks = []

        for task in dag_info.get('tasks', []):
            task_id = task.get('task_id', '')
            operator_type = task.get('operator_type', '').lower()

            # Check if this is a check operator
            check_type = None
            for op_pattern, check_cat in AirflowCheckDetector.CHECK_OPERATORS.items():
                if op_pattern in operator_type:
                    check_type = check_cat
                    break

            if not check_type:
                continue

            # Extract check configuration
            parameters = task.get('parameters', {})

            check_info = {
                'task_id': task_id,
                'check_type': check_type,
                'parameters': parameters,
            }

            # Extract specific fields based on check type
            if check_type == 'sql_column':
                check_info['table'] = parameters.get('table', 'unknown')
                check_info['column_mapping'] = parameters.get('column_mapping', {})

            elif check_type == 'sql_table':
                check_info['table'] = parameters.get('table', 'unknown')
                check_info['checks'] = parameters.get('checks', {})

            elif check_type == 'sql_custom':
                check_info['sql'] = parameters.get('sql', '')

            elif check_type == 'python':
                check_info['python_callable'] = parameters.get('python_callable', '')

            detected_checks.append(check_info)
            logger.debug(f"Detected {check_type} check operator: {task_id}")

        if detected_checks:
            logger.info(f"🔍 Detected {len(detected_checks)} Airflow check operator(s)")

        return detected_checks

    @staticmethod
    def generate_check_specs_metadata(checks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate check spec metadata from detected checks.

        This creates metadata for each individual check (e.g., per column).

        Args:
            checks: List of detected check operators

        Returns:
            List of check spec definitions with:
            - name: Unique check name
            - description: Human-readable description
            - check_type: Type of check
            - source_task: Original Airflow task ID
        """
        check_specs = []

        for check in checks:
            task_id = check['task_id']
            check_type = check['check_type']

            if check_type == 'sql_column':
                # One check spec per column
                table = check.get('table', 'unknown')
                column_mapping = check.get('column_mapping', {})

                for column, column_checks in column_mapping.items():
                    for check_name, check_config in column_checks.items():
                        spec = {
                            'name': f"airflow_{task_id}_{column}_{check_name}",
                            'description': f"Column check: {table}.{column} - {check_name}",
                            'check_type': 'sql_column',
                            'source_task': task_id,
                            'table': table,
                            'column': column,
                            'check_name': check_name,
                            'config': check_config,
                        }
                        check_specs.append(spec)

            elif check_type == 'sql_table':
                # One check spec per table check
                table = check.get('table', 'unknown')
                table_checks = check.get('checks', {})

                for check_name, check_config in table_checks.items():
                    spec = {
                        'name': f"airflow_{task_id}_{check_name}",
                        'description': f"Table check: {table} - {check_name}",
                        'check_type': 'sql_table',
                        'source_task': task_id,
                        'table': table,
                        'check_name': check_name,
                        'config': check_config,
                    }
                    check_specs.append(spec)

            elif check_type == 'sql_custom':
                # Single check for custom SQL
                spec = {
                    'name': f"airflow_{task_id}",
                    'description': f"SQL check: {task_id}",
                    'check_type': 'sql_custom',
                    'source_task': task_id,
                    'sql': check.get('sql', ''),
                }
                check_specs.append(spec)

            elif check_type == 'python':
                # Single check for Python callable
                spec = {
                    'name': f"airflow_{task_id}",
                    'description': f"Python check: {task_id}",
                    'check_type': 'python',
                    'source_task': task_id,
                    'callable': check.get('python_callable', ''),
                }
                check_specs.append(spec)

        return check_specs

    @staticmethod
    def parse_check_results_from_logs(log_output: str, check_specs: List[Dict[str, Any]]) -> List[AssetCheckResult]:
        """Parse Airflow logs to extract check results.

        Airflow check operators log results in specific formats:
        - INFO lines for passed checks
        - ERROR/WARNING lines for failed checks

        Args:
            log_output: Full Airflow DAG execution logs
            check_specs: List of expected check specs

        Returns:
            List of AssetCheckResult objects
        """
        results = []

        # Patterns to detect in logs
        # SQLColumnCheckOperator logs: "Column check passed: <table>.<column>"
        # SQLTableCheckOperator logs: "Table check passed: <table>"
        # Or: "Check failed: ..."

        # Build a map of task IDs to check specs
        task_to_checks = {}
        for spec in check_specs:
            task_id = spec['source_task']
            if task_id not in task_to_checks:
                task_to_checks[task_id] = []
            task_to_checks[task_id].append(spec)

        # Parse logs line by line
        for line in log_output.split('\n'):
            # Look for check operator results
            # Common patterns:
            # - "SQLColumnCheckOperator" in line
            # - "SQLTableCheckOperator" in line
            # - "passed" or "failed" in line

            if 'CheckOperator' not in line:
                continue

            # Try to extract check results
            # Pattern: [timestamp] {module} INFO/ERROR - Check <passed/failed>: <details>

            # Detect task ID from log line
            task_id_match = re.search(r"Task\[(\w+)\]", line)
            if not task_id_match:
                # Try alternative pattern: task_id in brackets
                task_id_match = re.search(r"\[(\w+)\]", line)

            if not task_id_match:
                continue

            task_id = task_id_match.group(1)

            # Check if this task has associated checks
            if task_id not in task_to_checks:
                continue

            # Determine if check passed or failed
            passed = None
            if ' INFO ' in line and 'passed' in line.lower():
                passed = True
            elif (' ERROR ' in line or ' WARNING ' in line) and 'failed' in line.lower():
                passed = False

            if passed is None:
                continue

            # Extract more details if possible
            # For column checks: look for column name
            # For table checks: look for table name

            column_match = re.search(r"column[:\s]+['\"]*(\w+)['\"]*", line, re.IGNORECASE)
            check_name_match = re.search(r"check[:\s]+['\"]*(\w+)['\"]*", line, re.IGNORECASE)

            # Match to specific check spec
            matched_specs = task_to_checks[task_id]

            for spec in matched_specs:
                # Try to match based on column/check name
                if column_match and spec.get('column') == column_match.group(1):
                    # Found matching column check
                    result = AssetCheckResult(
                        passed=passed,
                        metadata={
                            'check_type': spec['check_type'],
                            'source_task': task_id,
                            'table': spec.get('table'),
                            'column': spec.get('column'),
                            'check_name': spec.get('check_name'),
                            'log_line': line[:200],  # First 200 chars
                        }
                    )
                    results.append(result)
                    break
                elif spec['check_type'] in ['sql_table', 'sql_custom', 'python']:
                    # Table/custom/python checks don't have columns
                    result = AssetCheckResult(
                        passed=passed,
                        metadata={
                            'check_type': spec['check_type'],
                            'source_task': task_id,
                            'log_line': line[:200],
                        }
                    )
                    results.append(result)
                    break

        # If no results parsed but we have check specs, log a warning
        if check_specs and not results:
            logger.warning(
                f"Could not parse check results from logs. "
                f"Expected {len(check_specs)} checks but found 0 results."
            )

        return results

    @staticmethod
    def create_default_check_results(check_specs: List[Dict[str, Any]], passed: bool = True) -> List[AssetCheckResult]:
        """Create default check results when log parsing fails.

        This is a fallback for when we can't parse the logs.

        Args:
            check_specs: List of check specs
            passed: Default pass/fail status (default True - optimistic)

        Returns:
            List of AssetCheckResult objects with default status
        """
        results = []

        for spec in check_specs:
            result = AssetCheckResult(
                passed=passed,
                metadata={
                    'check_type': spec['check_type'],
                    'source_task': spec['source_task'],
                    'note': 'Default result - could not parse from logs',
                }
            )
            results.append(result)

        return results
