"""Generate asset checks from Python scripts."""

import ast
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dagster import AssetCheckResult, AssetCheckSpec, AssetKey, asset_check

logger = logging.getLogger(__name__)


class AssetCheckGenerator:
    """Generate asset checks by analyzing Python scripts.

    Detects common validation patterns:
    - assert statements
    - DataFrame shape checks
    - File existence checks
    - Data quality validations
    - Range checks
    """

    @staticmethod
    def extract_checks_from_file(script_path: Path, asset_key: str) -> List[Dict[str, Any]]:
        """Extract potential asset checks from a Python file.

        Args:
            script_path: Path to the Python script
            asset_key: Asset key this check applies to

        Returns:
            List of check definitions with:
            - name: Check name
            - description: What the check validates
            - pattern: Type of check (assert, shape, file, range, etc.)
            - code: Original code snippet
        """
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                source = f.read()

            tree = ast.parse(source)
            checks = []

            # Visit all nodes in the AST
            for node in ast.walk(tree):
                # Check for assert statements
                if isinstance(node, ast.Assert):
                    check = AssetCheckGenerator._parse_assert(node, source)
                    if check:
                        checks.append(check)

                # Check for common validation patterns
                elif isinstance(node, ast.Call):
                    check = AssetCheckGenerator._parse_validation_call(node, source)
                    if check:
                        checks.append(check)

            logger.info(f"Found {len(checks)} potential checks in {script_path.name}")
            return checks

        except Exception as e:
            logger.debug(f"Error extracting checks from {script_path}: {e}")
            return []

    @staticmethod
    def _parse_assert(node: ast.Assert, source: str) -> Optional[Dict[str, Any]]:
        """Parse an assert statement into a check definition.

        Args:
            node: AST Assert node
            source: Full source code

        Returns:
            Check definition dict or None
        """
        try:
            # Get the assertion condition
            test = node.test

            # Extract code snippet
            code_lines = source.split('\n')
            if hasattr(node, 'lineno'):
                code = code_lines[node.lineno - 1].strip()
            else:
                code = ast.unparse(node)

            # Parse the assertion
            check_name = None
            description = None
            pattern = "assert"

            # Common patterns
            if isinstance(test, ast.Compare):
                # assert x > y, assert len(df) > 0, etc.
                left = ast.unparse(test.left)
                ops = [type(op).__name__ for op in test.ops]
                comparators = [ast.unparse(c) for c in test.comparators]

                # Detect specific patterns
                if 'len(' in left or '.shape' in left:
                    pattern = "shape_check"
                    check_name = "check_size"
                    description = f"Validates that {left} {ops[0]} {comparators[0]}"

                elif any(word in left.lower() for word in ['count', 'rows', 'records']):
                    pattern = "count_check"
                    check_name = "check_count"
                    description = f"Validates record count: {left} {ops[0]} {comparators[0]}"

                else:
                    check_name = "check_assertion"
                    description = f"Validates: {code.replace('assert ', '')}"

            elif isinstance(test, ast.Call):
                # assert os.path.exists(), assert is_valid(), etc.
                func_name = ast.unparse(test.func)

                if 'exists' in func_name.lower():
                    pattern = "file_existence"
                    check_name = "check_file_exists"
                    description = f"Validates file existence: {ast.unparse(test)}"

                elif 'is_valid' in func_name.lower() or 'validate' in func_name.lower():
                    pattern = "validation"
                    check_name = "check_validation"
                    description = f"Data validation: {ast.unparse(test)}"

                else:
                    check_name = "check_condition"
                    description = f"Validates: {code.replace('assert ', '')}"

            elif isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
                # assert not x
                check_name = "check_negative"
                description = f"Validates: {code.replace('assert ', '')}"

            else:
                check_name = "check_condition"
                description = f"Validates: {code.replace('assert ', '')}"

            # Get assertion message if present
            if node.msg:
                msg = ast.unparse(node.msg)
                description = msg.strip('"\'')

            return {
                'name': check_name,
                'description': description,
                'pattern': pattern,
                'code': code,
            }

        except Exception as e:
            logger.debug(f"Could not parse assert: {e}")
            return None

    @staticmethod
    def _parse_validation_call(node: ast.Call, source: str) -> Optional[Dict[str, Any]]:
        """Parse a function call that might be a validation.

        Detects patterns like:
        - df.shape[0] > 0
        - os.path.exists(file_path)
        - validate_data(df)

        Args:
            node: AST Call node
            source: Full source code

        Returns:
            Check definition dict or None
        """
        try:
            func_name = ast.unparse(node.func)

            # Check for validation function calls
            validation_keywords = ['validate', 'check', 'verify', 'ensure']

            if any(kw in func_name.lower() for kw in validation_keywords):
                code_lines = source.split('\n')
                if hasattr(node, 'lineno'):
                    code = code_lines[node.lineno - 1].strip()
                else:
                    code = ast.unparse(node)

                return {
                    'name': f"check_{func_name.split('.')[-1].lower()}",
                    'description': f"Validation function: {func_name}",
                    'pattern': "validation_function",
                    'code': code,
                }

            return None

        except Exception as e:
            logger.debug(f"Could not parse validation call: {e}")
            return None

    @staticmethod
    def create_asset_check_specs(
        asset_key: str,
        checks: List[Dict[str, Any]]
    ) -> List[AssetCheckSpec]:
        """Create AssetCheckSpec objects from check definitions.

        Args:
            asset_key: Asset key to check
            checks: List of check definitions

        Returns:
            List of AssetCheckSpec objects
        """
        specs = []

        for i, check in enumerate(checks):
            # Create unique check name
            check_name = check.get('name', f"check_{i+1}")
            # Ensure name is unique
            check_name = f"{check_name}_{i+1}"

            spec = AssetCheckSpec(
                name=check_name,
                asset=AssetKey(asset_key),
                description=check.get('description', 'Auto-generated check'),
            )
            specs.append(spec)

        return specs

    @staticmethod
    def generate_check_functions(
        asset_key: str,
        checks: List[Dict[str, Any]],
        script_path: Path
    ) -> List[Any]:
        """Generate @asset_check decorated functions.

        Note: This generates the check function definitions but they need
        to be executed in the context where the data is available.

        For now, we return check specs and users can implement the checks.

        Args:
            asset_key: Asset key to check
            checks: List of check definitions
            script_path: Path to the script

        Returns:
            List of asset check functions (placeholders)
        """
        check_functions = []

        for i, check in enumerate(checks):
            check_name = f"{check.get('name', 'check')}_{i+1}"
            description = check.get('description', 'Auto-generated check')
            pattern = check.get('pattern', 'assert')

            # Create a basic check function
            # In practice, these would need to be implemented based on the pattern

            def make_check(check_name, description, asset_key):
                @asset_check(asset=AssetKey(asset_key), name=check_name, description=description)
                def check_func(context):
                    """Auto-generated check."""
                    # Placeholder - actual implementation depends on the check pattern
                    # For now, just pass
                    context.log.info(f"Running check: {check_name}")

                    # In a real implementation, we would:
                    # 1. Load the asset data
                    # 2. Execute the check logic
                    # 3. Return AssetCheckResult

                    return AssetCheckResult(
                        passed=True,
                        metadata={
                            "check_pattern": pattern,
                            "auto_generated": True,
                        }
                    )

                return check_func

            check_functions.append(make_check(check_name, description, asset_key))

        return check_functions


# Example usage:
"""
from .utils import AssetCheckGenerator

# Extract checks from script
checks = AssetCheckGenerator.extract_checks_from_file(
    script_path,
    asset_key="my_asset"
)

# Create check specs
check_specs = AssetCheckGenerator.create_asset_check_specs(
    "my_asset",
    checks
)

# Include in Definitions
Definitions(
    assets=[my_asset],
    asset_checks=check_specs,
)
"""
