"""
Build script for marimo notebooks.

This script exports marimo notebooks to HTML/WebAssembly format and generates
an index.html file that lists all the notebooks. It handles both regular notebooks
(from the notebooks/ directory).

The script can be run from the command line with optional arguments:
    uv run .github/scripts/build.py [--output-dir OUTPUT_DIR]

The exported files will be placed in the specified output directory (default: _site).
"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "jinja2==3.1.3",
#     "fire==0.7.0",
#     "loguru==0.7.0"
# ]
# ///

import shutil
import subprocess
from typing import List, Union
from pathlib import Path
import json

import jinja2
import fire

from loguru import logger

def _export_html_wasm(notebook_path: Path, output_dir: Path, format: str = 'app') -> bool:
    """Export a single marimo notebook to HTML/WebAssembly format.

    This function takes a marimo notebook (.py file) and exports it to HTML/WebAssembly format.
    If as_app is True, the notebook is exported in "run" mode with code hidden, suitable for
    applications. Otherwise, it's exported in "edit" mode, suitable for interactive notebooks.

    Args:
        notebook_path (Path): Path to the marimo notebook (.py file) to export
        output_dir (Path): Directory where the exported HTML file will be saved
        as_app (bool, optional): Whether to export as an app (run mode) or notebook (edit mode).
                                Defaults to False.

    Returns:
        bool: True if export succeeded, False otherwise
    """
    # Convert .py extension to .html for the output file
    output_path: Path = Path(notebook_path.parent.name).with_suffix(".html")

    # Base command for marimo export
    if format == 'html':
        logger.info(f"Exporting {notebook_path} to {output_path} as html")
        cmd: List[str] = ["uvx", "marimo", "export", "html", "--sandbox", "--no-include-code"]
    elif format == 'app':
        logger.info(f"Exporting {notebook_path} to {output_path} as app")
        # Apps run in "run" mode with hidden code
        cmd: List[str] = ["uvx", "marimo", "export", "html-wasm", "--sandbox", "--mode", "run", "--no-show-code"]
    elif format == 'notebook':
        logger.info(f"Exporting {notebook_path} to {output_path} as notebook")
        # Notebooks run in "edit" mode
        cmd: List[str] = ["uvx", "marimo", "export", "html-wasm", "--sandbox", "--mode", "edit"]
    else:
         logger.error(f'format "{format}" not recognized, needs to be one off html, app, notebook')
         return False

    try:
        # Create full output path and ensure directory exists
        output_file: Path = output_dir / Path(notebook_path.parent.name).with_suffix(".html")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Add notebook path and output file to command
        cmd.extend([str(notebook_path), "-o", str(output_file)])

        # Run marimo export command
        logger.debug(f"Running command: {cmd}")
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info(f"Successfully exported {notebook_path}")
        return True
    except subprocess.CalledProcessError as e:
        # Handle marimo export errors
        logger.error(f"Error exporting {notebook_path}:")
        logger.error(f"Command output: {e.stderr}")
        return False
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Unexpected error exporting {notebook_path}: {e}")
        return False


def _generate_index(output_dir: Path, template_file: Path, notebooks_data: List[dict] | None = None) -> None:
    """Generate an index.html file that lists all the notebooks.

    This function creates an HTML index page that displays links to all the exported
    notebooks. The index page includes the marimo logo and displays each notebook
    with a formatted title and a link to open it.

    Args:
        notebooks_data (List[dict]): List of dictionaries with data for notebooks
        output_dir (Path): Directory where the index.html file will be saved
        template_file (Path, optional): Path to the template file. If None, uses the default template.

    Returns:
        None
    """
    logger.info("Generating index.html")

    # Create the full path for the index.html file
    index_path: Path = output_dir / "index.html"

    # Ensure the output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Set up Jinja2 environment and load template
        template_dir = template_file.parent
        template_name = template_file.name
        env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(template_dir),
            autoescape=jinja2.select_autoescape(["html", "xml"])
        )
        template = env.get_template(template_name)

        # Render the template with notebook data
        rendered_html = template.render(notebooks=notebooks_data)

        # Write the rendered HTML to the index.html file
        with open(index_path, "w") as f:
            f.write(rendered_html)
        logger.info(f"Successfully generated index.html at {index_path}")

    except IOError as e:
        # Handle file I/O errors
        logger.error(f"Error generating index.html: {e}")
    except jinja2.exceptions.TemplateError as e:
        # Handle template errors
        logger.error(f"Error rendering template: {e}")


def _export(folder: Path, output_dir: Path) -> List[dict]:
    """Export all marimo notebooks in a folder to HTML/WebAssembly format.

    This function finds all Python files in the specified folder and exports them
    to HTML/WebAssembly format using the export_html_wasm function. It returns a
    list of dictionaries containing the data needed for the template.

    Args:
        folder (Path): Path to the folder containing marimo notebooks
        output_dir (Path): Directory where the exported HTML files will be saved

    Returns:
        List[dict]: List of dictionaries with "display_name" and "html_path" for each notebook
    """
    # Check if the folder exists
    if not folder.exists():
        logger.warning(f"Directory not found: {folder}")
        return []

    # Find all Python files recursively in the folder
    notebooks = list(item for item in folder.iterdir() if item.is_dir())
    logger.debug(f"Found {len(notebooks)} Python files in {folder}")

    # Exit if no notebooks were found
    if not notebooks:
        logger.warning(f"No notebooks found in {folder}!")
        return []

    # For each successfully exported notebook, add its data to the notebook_data list
    notebook_data = [
        _get_metadata(notebook)
        for notebook in notebooks
        if _export_html_wasm(notebook / 'notebook.py', output_dir, format=_get_metadata(notebook).get('format', 'app'))
    ]

    # Copy each notebook's public/ folder into _site/ so index page thumbnails resolve correctly.
    # marimo export html-wasm does this automatically for WASM notebooks, but not for html format.
    for notebook in notebooks:
        public_src = notebook / 'public'
        if public_src.is_dir():
            shutil.copytree(public_src, output_dir / 'public', dirs_exist_ok=True)

    logger.info(f"Successfully exported {len(notebook_data)} out of {len(notebooks)} files from {folder}")
    return notebook_data

def _get_metadata(notebook):
    with open(notebook / 'metadata.json') as f:
        metadata = json.load(f)
    metadata['html_path'] = str(Path(notebook.name).with_suffix(".html"))
    return metadata


def main(
    output_dir: Union[str, Path] = "_site",
    template: Union[str, Path] = "index.html.j2",
) -> None:
    """Main function to export marimo notebooks.

    This function:
    1. Parses command line arguments
    2. Exports all marimo notebooks in the 'notebooks' directories
    3. Generates an index.html file that lists all the notebooks

    Command line arguments:
        --output-dir: Directory where the exported files will be saved (default: _site)
        --template: Path to the template file (default: templates/index.html.j2)

    Returns:
        None
    """
    logger.info("Starting marimo build process")

    # Convert output_dir explicitly to Path (not done by fire)
    output_dir: Path = Path(output_dir)
    logger.info(f"Output directory: {output_dir}")

    # Make sure the output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert template to Path if provided
    template_file: Path = Path(template)
    logger.info(f"Using template file: {template_file}")

    # Export notebooks from the notebooks
    notebooks_data = _export(Path("notebooks"), output_dir)

    # Exit if no notebooks
    if not notebooks_data:
        logger.warning("No notebooks found!")

    # Generate the index.html file that lists all notebooks
    _generate_index(output_dir=output_dir, notebooks_data=notebooks_data, template_file=template_file)

    logger.info(f"Build completed successfully. Output directory: {output_dir}")


if __name__ == '__main__':
    fire.Fire(main)
