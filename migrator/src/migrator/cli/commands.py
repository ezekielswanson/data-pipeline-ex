import click
import subprocess

from pathlib import Path

from migrator.utils.config import ConfigPackLoader
from migrator.utils.logger import get_logger
from migrator.transform.transform import DataTransformer

logger = get_logger()

@click.group()
def cli():
    """HubSpot Migration Tool"""
    pass

@cli.command()
@click.argument('directory', required=False)
@click.option('-v', '--verbose', is_flag=True, help='Enable verbose output')
@click.option('--no-cov', is_flag=True, help='Disable coverage reporting')
@click.option('--dev', is_flag=True, help='Run the development test instead of pytest')
def test(directory, verbose, no_cov, dev):
    """Run tests - either pytest tests or the development test"""
    if dev:
        # Run the original development test
        logger.info("Running development test command")
        click.echo("Test command executed successfully")
        return 0
    
    # Run pytest tests
    logger.info(f"Running tests{f' in {directory}' if directory else ''}")
    
    # Base command
    cmd = ["python", "-m", "pytest"]
    
    # Add directory if specified
    if directory:
        if directory.startswith("test_"):
            # If it's a test module name, look for it in tests directory
            cmd.append(f"tests/{directory}")
        else:
            # Otherwise use the directory as-is
            cmd.append(directory)
    
    # Add verbose flag if requested
    if verbose:
        cmd.append("-v")
    
    # Disable coverage if requested
    if no_cov:
        cmd.append("--no-cov")
    
    # Run the command
    logger.info(f"Running command: {' '.join(cmd)}")
    click.echo(f"Running: {' '.join(cmd)}")
    result = subprocess.call(cmd)
    
    if result == 0:
        click.echo("Tests completed successfully")
    else:
        click.echo("Tests failed")
    
    return result

@cli.command()
@click.option(
    '--object-type',
    type=click.Choice(['contacts', 'companies', 'deals', 'engagements', 'files', 'all']),
    required=True,
    help='Type of object to extract'
)
@click.option(
    '--config-pack',
    type=click.Path(exists=True),
    default='../../config/config-pack',
    help='Path to configuration pack'
)
@click.option(
    '--incremental',
    is_flag=True,
    help='Perform incremental extract based on last run'
)
def extract(object_type: str, config_pack: str, incremental: bool):
    """Extract data from source HubSpot portal"""
    logger.info(f"Starting extraction for {object_type}")
    loader = ConfigPackLoader(config_pack)
    
    if object_type == 'all':
        for obj_type in loader.config['objects'].keys():
            _extract_object(obj_type, loader, incremental)
    else:
        _extract_object(object_type, loader, incremental)

@cli.command()
@click.option(
    '--object-type',
    type=click.Choice(['contacts', 'companies', 'deals', 'engagements', 'files', 'all']),
    required=True,
    help='Type of object to transform'
)
@click.option(
    '--config-pack',
    type=click.Path(exists=True),
    default='../../config/config-pack',
    help='Path to configuration pack'
)
def transform(object_type: str, config_pack: str):
    """Transform extracted data using configuration rules"""
    logger.info(f"Starting transformation for {object_type}")
    loader = ConfigPackLoader(config_pack)
    
    # Slight variation from original code in that we don't check for piped input if object_type is 'all'
    # Don't want to allow for piped input for all object types, and we don't want to check for piped input if object_type is 'all'
    # If object_type is 'all', we want to instead use the config pack to get the object types
    is_piped = not click.get_text_stream('stdin').isatty()
    io_config = {
        'input_stream': click.get_text_stream('stdin'),
        'output_stream': click.get_text_stream('stdout')
    } if is_piped and object_type != 'all' else {}
    
    if object_type == 'all':
        for obj_type in loader.config['objects'].keys():
            _transform_object(obj_type, loader)
    else:
        _transform_object(object_type, loader, **io_config)
@cli.command()
@click.option(
    '--object-type',
    type=click.Choice(['contacts', 'companies', 'deals', 'engagements', 'files', 'all']),
    required=True,
    help='Type of object to load'
)
@click.option(
    '--config-pack',
    type=click.Path(exists=True),
    default='../../config/config-pack',
    help='Path to configuration pack'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Validate load operation without making changes'
)
def load(object_type: str, config_pack: str, dry_run: bool):
    """Load transformed data into target HubSpot portal"""
    logger.info(f"Starting load for {object_type}")
    loader = ConfigPackLoader(config_pack)
    
    if object_type == 'all':
        for obj_type in loader.config['objects'].keys():
            _load_object(obj_type, loader, dry_run)
    else:
        _load_object(object_type, loader, dry_run)

@cli.command()
@click.option(
    '--object-type',
    type=click.Choice(['contacts', 'companies', 'deals', 'engagements', 'files', 'all']),
    required=True,
    help='Type of object to migrate'
)
@click.option(
    '--config-pack',
    type=click.Path(exists=True),
    default='../../config/config-pack',
    help='Path to configuration pack'
)
@click.option(
    '--incremental',
    is_flag=True,
    help='Perform incremental migration'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Validate operations without making changes'
)
def migrate(object_type: str, config_pack: str, incremental: bool, dry_run: bool):
    """Execute full migration process (extract, transform, load)"""
    logger.info(f"Starting migration for {object_type}")
    loader = ConfigPackLoader(config_pack)
    
    if object_type == 'all':
        for obj_type in loader.config['objects'].keys():
            _run_migration(obj_type, loader, incremental, dry_run)
    else:
        _run_migration(object_type, loader, incremental, dry_run)

def _extract_object(object_type: str, loader: ConfigPackLoader, incremental: bool):
    """Handle extraction for a single object type"""
    logger.info(f"Extracting {object_type}")
    # Implementation to come...

def _transform_object(object_type: str, loader: ConfigPackLoader, input_stream=None, output_stream=None):
    """Handle transformation for a single object type"""
    logger.info(f"Transforming {object_type}")
    
    try:
        # Load configuration
        object_config = loader.load_object_config(object_type)
        transformations = object_config['transformations']
   
        transformer = DataTransformer(transformations)
        
        config_dir = loader.config_pack_path / object_type
        input_path = None if input_stream else Path(f"data/{object_type}_extracted.csv")
        output_path = None if output_stream else Path(f"data/{object_type}_transformed.csv")
        
        transformer.transform_file(
            input_path=input_path,
            output_path=output_path,
            input_stream=input_stream,
            output_stream=output_stream,
            config_dir=str(config_dir)
        )
        
    except Exception as e:
        logger.error(f"Error transforming {object_type}: {str(e)}")

def _load_object(object_type: str, loader: ConfigPackLoader, dry_run: bool):
    """Handle loading for a single object type"""
    logger.info(f"Loading {object_type}")
    # Implementation to come...

def _run_migration(object_type: str, loader: ConfigPackLoader, incremental: bool, dry_run: bool):
    """Run full migration process for a single object type"""
    _extract_object(object_type, loader, incremental)
    _transform_object(object_type, loader)
    _load_object(object_type, loader, dry_run)

if __name__ == '__main__':
    cli()