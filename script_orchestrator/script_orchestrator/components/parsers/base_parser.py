"""Base parser utilities shared by Prefect and Airflow parsers."""

import ast
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


class BaseParser:
    """Base class with shared parsing utilities."""

    @staticmethod
    def has_decorator(func_node: ast.FunctionDef, decorator_name: str) -> bool:
        """Check if function has a specific decorator."""
        for decorator in func_node.decorator_list:
            if isinstance(decorator, ast.Name) and decorator.id == decorator_name:
                return True
            elif isinstance(decorator, ast.Call):
                if isinstance(decorator.func, ast.Name) and decorator.func.id == decorator_name:
                    return True
        return False

    @staticmethod
    def has_return_statement(func_node: ast.FunctionDef) -> bool:
        """Check if function has a return statement."""
        for node in ast.walk(func_node):
            if isinstance(node, ast.Return) and node.value is not None:
                return True
        return False

    @staticmethod
    def extract_function_parameters(func_node: ast.FunctionDef) -> List[Dict]:
        """Extract parameters from a function signature."""
        parameters = []

        for arg in func_node.args.args:
            if arg.arg == 'self':
                continue

            param_info = {
                'name': arg.arg,
                'type_annotation': None,
                'default': None
            }

            # Extract type annotation
            if arg.annotation:
                param_info['type_annotation'] = ast.unparse(arg.annotation)

            parameters.append(param_info)

        # Extract default values
        defaults = func_node.args.defaults
        num_defaults = len(defaults)
        num_args = len(parameters)

        for i, default in enumerate(defaults):
            param_index = num_args - num_defaults + i
            if param_index >= 0 and param_index < len(parameters):
                try:
                    parameters[param_index]['default'] = ast.literal_eval(default)
                except (ValueError, SyntaxError):
                    parameters[param_index]['default'] = None

        return parameters

    @staticmethod
    def extract_decorator_kwargs(decorator: ast.Call) -> Dict:
        """Extract keyword arguments from a decorator call."""
        kwargs = {}
        for keyword in decorator.keywords:
            if keyword.arg:
                try:
                    kwargs[keyword.arg] = ast.literal_eval(keyword.value)
                except (ValueError, SyntaxError, TypeError):
                    # For non-literal values, store the AST node
                    kwargs[keyword.arg] = keyword.value
        return kwargs
