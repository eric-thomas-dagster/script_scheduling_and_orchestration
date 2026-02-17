"""Extract documentation and metadata from Python scripts."""

import ast
import logging
import re
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class DocumentationExtractor:
    """Extract structured documentation from Python scripts.

    Parses docstrings and extracts structured metadata like:
    - Description
    - Input/Output information
    - Owner/Team
    - Schedule information
    - Tags
    - SLAs
    """

    # Patterns to detect structured metadata in docstrings
    METADATA_PATTERNS = {
        'input': r'(?i)^input[s]?:\s*(.+)$',
        'output': r'(?i)^output[s]?:\s*(.+)$',
        'owner': r'(?i)^owner:\s*(.+)$',
        'team': r'(?i)^team:\s*(.+)$',
        'schedule': r'(?i)^schedule:\s*(.+)$',
        'sla': r'(?i)^sla:\s*(.+)$',
        'depends_on': r'(?i)^depends[_ ]on:\s*(.+)$',
        'produces': r'(?i)^produces:\s*(.+)$',
    }

    @staticmethod
    def extract_from_file(script_path: Path) -> Dict[str, Any]:
        """Extract documentation and metadata from a Python file.

        Args:
            script_path: Path to the Python script

        Returns:
            Dict containing:
            - description: Main description text
            - metadata: Structured metadata fields
            - has_documentation: Whether any docs were found
        """
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                source = f.read()

            # Parse the AST
            tree = ast.parse(source)

            # Extract module-level docstring
            module_docstring = ast.get_docstring(tree)

            # Parse the docstring for structured metadata
            if module_docstring:
                return DocumentationExtractor._parse_docstring(module_docstring)
            else:
                return {
                    'description': None,
                    'metadata': {},
                    'has_documentation': False,
                }

        except Exception as e:
            logger.debug(f"Error extracting documentation from {script_path}: {e}")
            return {
                'description': None,
                'metadata': {},
                'has_documentation': False,
            }

    @staticmethod
    def _parse_docstring(docstring: str) -> Dict[str, Any]:
        """Parse a docstring and extract structured metadata.

        Format:
        ```
        This is the main description.

        Input: customer_data
        Output: processed_data
        Owner: data-team@company.com
        Schedule: Daily at 2 AM
        SLA: 4 hours
        Tags: etl, customers
        ```

        Args:
            docstring: The docstring text

        Returns:
            Dict with 'description', 'metadata', and 'has_documentation'
        """
        lines = docstring.split('\n')

        description_lines = []
        metadata = {}
        in_description = True

        for line in lines:
            line = line.strip()

            if not line:
                continue

            # Check if this line matches any metadata pattern
            matched = False
            for key, pattern in DocumentationExtractor.METADATA_PATTERNS.items():
                match = re.match(pattern, line)
                if match:
                    metadata[key] = match.group(1).strip()
                    matched = True
                    in_description = False
                    break

            # Check for Tags pattern
            if not matched and re.match(r'(?i)^tags?:\s*(.+)$', line):
                match = re.match(r'(?i)^tags?:\s*(.+)$', line)
                tags_str = match.group(1).strip()
                # Parse comma-separated tags
                tags = [t.strip() for t in tags_str.split(',')]
                metadata['tags'] = tags
                matched = True
                in_description = False

            # If not a metadata line and we're still in description, add to description
            if not matched and in_description:
                description_lines.append(line)

        description = ' '.join(description_lines).strip()

        return {
            'description': description if description else None,
            'metadata': metadata,
            'has_documentation': bool(description or metadata),
        }

    @staticmethod
    def extract_function_docs(script_path: Path, function_name: str) -> Optional[str]:
        """Extract docstring from a specific function.

        Args:
            script_path: Path to the Python script
            function_name: Name of the function

        Returns:
            Function docstring or None
        """
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                source = f.read()

            tree = ast.parse(source)

            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name == function_name:
                    return ast.get_docstring(node)

            return None

        except Exception as e:
            logger.debug(f"Error extracting function docs from {script_path}: {e}")
            return None

    @staticmethod
    def enrich_asset_metadata(
        base_metadata: Dict[str, Any],
        doc_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Enrich asset metadata with extracted documentation.

        Args:
            base_metadata: Existing metadata dict
            doc_info: Documentation info from extract_from_file

        Returns:
            Enhanced metadata dict
        """
        enriched = base_metadata.copy()

        # Add structured metadata fields
        for key, value in doc_info.get('metadata', {}).items():
            # Use metadata key as-is, prefixed with doc_
            enriched[f'doc_{key}'] = value

        return enriched

    @staticmethod
    def create_rich_description(
        script_name: str,
        doc_info: Dict[str, Any],
        fallback_description: Optional[str] = None
    ) -> str:
        """Create a rich description for an asset.

        Args:
            script_name: Name of the script
            doc_info: Documentation info from extract_from_file
            fallback_description: Fallback if no docs found

        Returns:
            Formatted description string
        """
        description = doc_info.get('description')

        if description:
            return description

        # If no description in docstring, use fallback
        if fallback_description:
            return fallback_description

        # Last resort: generate from script name
        return f"Asset from script: {script_name}"


# Example usage:
"""
from .utils import DocumentationExtractor

# Extract docs from script
doc_info = DocumentationExtractor.extract_from_file(script_path)

# Create asset with rich metadata
@asset(
    description=DocumentationExtractor.create_rich_description(
        script_info.name, doc_info, metadata.description
    ),
    metadata=DocumentationExtractor.enrich_asset_metadata(
        base_metadata, doc_info
    )
)
def my_asset(context):
    ...
"""
