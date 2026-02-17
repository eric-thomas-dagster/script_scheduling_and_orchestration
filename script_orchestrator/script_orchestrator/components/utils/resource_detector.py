"""Auto-detect and generate Dagster resources from Python script imports."""

import ast
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# Known resource patterns: import_name → (resource_name, resource_type, config_keys)
RESOURCE_PATTERNS = {
    # Database
    'psycopg2': ('postgres', 'database', ['host', 'port', 'database', 'user', 'password']),
    'pymysql': ('mysql', 'database', ['host', 'port', 'database', 'user', 'password']),
    'mysql.connector': ('mysql', 'database', ['host', 'port', 'database', 'user', 'password']),
    'sqlite3': ('sqlite', 'database', ['database']),
    'sqlalchemy': ('sqlalchemy', 'database', ['connection_string']),

    # Cloud Storage
    'boto3': ('s3', 'storage', ['aws_access_key_id', 'aws_secret_access_key', 'region_name']),
    'google.cloud.storage': ('gcs', 'storage', ['project_id', 'credentials_path']),
    'azure.storage.blob': ('azure_blob', 'storage', ['account_name', 'account_key']),

    # APIs
    'requests': ('http', 'api', ['base_url', 'timeout', 'headers']),
    'httpx': ('http', 'api', ['base_url', 'timeout']),

    # Message Queues
    'pika': ('rabbitmq', 'queue', ['host', 'port', 'username', 'password']),
    'kafka': ('kafka', 'queue', ['bootstrap_servers']),
    'redis': ('redis', 'cache', ['host', 'port', 'password']),

    # Data Processing
    'pandas': ('pandas', 'dataframe', []),
    'pyspark': ('spark', 'compute', ['master', 'app_name']),
    'dask': ('dask', 'compute', ['scheduler_address']),

    # ML/AI
    'tensorflow': ('tensorflow', 'ml', []),
    'torch': ('pytorch', 'ml', []),
    'sklearn': ('scikit_learn', 'ml', []),

    # Monitoring
    'datadog': ('datadog', 'monitoring', ['api_key', 'app_key']),
    'prometheus_client': ('prometheus', 'monitoring', ['port']),

    # Communication
    'slack_sdk': ('slack', 'communication', ['token']),
    'sendgrid': ('sendgrid', 'communication', ['api_key']),
    'twilio': ('twilio', 'communication', ['account_sid', 'auth_token']),
}


class ResourceDetector:
    """Detect resource usage from Python scripts and generate resource definitions."""

    @staticmethod
    def detect_resources_from_file(script_path: Path) -> List[Dict[str, Any]]:
        """Detect resources used in a Python script.

        Args:
            script_path: Path to the Python script

        Returns:
            List of detected resources with:
            - import_name: The imported module
            - resource_name: Suggested resource name
            - resource_type: Type of resource (database, storage, etc.)
            - config_keys: Required configuration keys
            - usage_examples: Where/how it's used in the script
        """
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                source = f.read()

            tree = ast.parse(source)
            detected = []

            # Track imports
            imports = ResourceDetector._extract_imports(tree)

            # Check each import against known patterns
            for import_name in imports:
                # Check for exact match
                if import_name in RESOURCE_PATTERNS:
                    resource_name, resource_type, config_keys = RESOURCE_PATTERNS[import_name]
                    detected.append({
                        'import_name': import_name,
                        'resource_name': resource_name,
                        'resource_type': resource_type,
                        'config_keys': config_keys,
                        'usage_examples': ResourceDetector._find_usage_examples(tree, import_name),
                    })
                    continue

                # Check for partial matches (e.g., google.cloud.storage.client)
                for pattern, (resource_name, resource_type, config_keys) in RESOURCE_PATTERNS.items():
                    if import_name.startswith(pattern + '.') or pattern.startswith(import_name):
                        detected.append({
                            'import_name': import_name,
                            'resource_name': resource_name,
                            'resource_type': resource_type,
                            'config_keys': config_keys,
                            'usage_examples': [],
                        })
                        break

            if detected:
                logger.info(f"Detected {len(detected)} resources in {script_path.name}: "
                           f"{[r['resource_name'] for r in detected]}")

            return detected

        except Exception as e:
            logger.debug(f"Error detecting resources from {script_path}: {e}")
            return []

    @staticmethod
    def _extract_imports(tree: ast.AST) -> List[str]:
        """Extract all import names from AST.

        Args:
            tree: Python AST

        Returns:
            List of imported module names
        """
        imports = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)

            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
                    # Also add submodules
                    for alias in node.names:
                        imports.append(f"{node.module}.{alias.name}")

        return imports

    @staticmethod
    def _find_usage_examples(tree: ast.AST, import_name: str) -> List[str]:
        """Find usage examples of an imported module.

        Args:
            tree: Python AST
            import_name: Module name

        Returns:
            List of usage examples (code snippets)
        """
        examples = []

        # Get the module alias if any
        module_alias = None
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == import_name and alias.asname:
                        module_alias = alias.asname
                        break

        # Look for function calls
        search_name = module_alias if module_alias else import_name.split('.')[0]

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func_name = ast.unparse(node.func) if hasattr(ast, 'unparse') else str(node.func)
                if search_name in func_name:
                    examples.append(func_name)
                    if len(examples) >= 3:  # Limit to 3 examples
                        break

        return examples

    @staticmethod
    def generate_resource_code(
        resource_name: str,
        resource_type: str,
        config_keys: List[str],
        import_name: str
    ) -> str:
        """Generate Dagster resource code.

        Args:
            resource_name: Name for the resource
            resource_type: Type of resource
            config_keys: Configuration keys needed
            import_name: Python import statement

        Returns:
            Generated Python code for the resource
        """
        # Build config class if needed
        config_class_code = ""
        config_param = ""

        if config_keys:
            config_fields = "\n    ".join([
                f"{key}: str" for key in config_keys
            ])
            config_class_code = f"""
class {resource_name.capitalize()}Config(Config):
    \"\"\"Configuration for {resource_name} resource.\"\"\"
    {config_fields}
"""
            config_param = f"config: {resource_name.capitalize()}Config"

        # Build resource function
        resource_code = f"""{config_class_code}
@resource
def {resource_name}_resource({config_param}):
    \"\"\"Auto-generated {resource_type} resource.

    Detected from import: {import_name}

    Usage in asset:
    @asset(required_resource_keys={{"{resource_name}"}})
    def my_asset(context):
        {resource_name} = context.resources.{resource_name}
        # Use {resource_name} here
    \"\"\"
    import {import_name}

    # TODO: Initialize and return the resource
    # Example initialization based on type:
"""

        # Add type-specific initialization hints
        if resource_type == 'database':
            if 'psycopg2' in import_name:
                resource_code += f"""
    if config:
        return {import_name}.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
        )
    return {import_name}  # Return module if no config
"""
            elif 'sqlalchemy' in import_name:
                resource_code += f"""
    if config:
        from sqlalchemy import create_engine
        return create_engine(config.connection_string)
    return {import_name}  # Return module if no config
"""
            else:
                resource_code += f"""
    # Initialize database connection
    # return {import_name}.connect(**config_dict)
    return {import_name}
"""

        elif resource_type == 'storage':
            if 'boto3' in import_name:
                resource_code += f"""
    if config:
        return {import_name}.client(
            's3',
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.region_name,
        )
    return {import_name}  # Return module if no config
"""
            else:
                resource_code += f"""
    # Initialize storage client
    # return {import_name}.Client(**config_dict)
    return {import_name}
"""

        elif resource_type == 'api':
            resource_code += f"""
    if config:
        # Return configured session
        import {import_name}
        session = {import_name}.Session()
        if hasattr(config, 'base_url'):
            session.base_url = config.base_url
        if hasattr(config, 'headers'):
            session.headers.update(config.headers)
        return session
    return {import_name}  # Return module if no config
"""

        else:
            resource_code += f"""
    # Return the module or initialize based on your needs
    return {import_name}
"""

        return resource_code

    @staticmethod
    def generate_resources_file(
        detected_resources: List[Dict[str, Any]],
        output_path: Path
    ) -> None:
        """Generate a resources.py file with all detected resources.

        Args:
            detected_resources: List of detected resources
            output_path: Path to write the resources file
        """
        code_parts = [
            '"""Auto-generated Dagster resources."""\n',
            'from dagster import Config, resource\n\n',
        ]

        for resource_info in detected_resources:
            resource_code = ResourceDetector.generate_resource_code(
                resource_info['resource_name'],
                resource_info['resource_type'],
                resource_info['config_keys'],
                resource_info['import_name'],
            )
            code_parts.append(resource_code)
            code_parts.append('\n\n')

        # Write to file
        with open(output_path, 'w') as f:
            f.writelines(code_parts)

        logger.info(f"Generated resources file: {output_path}")


# Example usage:
"""
from .utils import ResourceDetector

# Detect resources from script
detected = ResourceDetector.detect_resources_from_file(script_path)

# Generate resource definitions
for resource in detected:
    code = ResourceDetector.generate_resource_code(
        resource['resource_name'],
        resource['resource_type'],
        resource['config_keys'],
        resource['import_name']
    )
    print(code)

# Or generate a resources.py file
ResourceDetector.generate_resources_file(
    detected,
    output_path=Path("resources.py")
)
"""
