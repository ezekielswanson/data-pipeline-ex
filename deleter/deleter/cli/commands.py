#!/usr/bin/env python3
import os
import click
import csv

from deleter.utils.logger import LoggerConfig

# Initialize logger with default settings
logger_config = LoggerConfig()
logger = logger_config.setup_logging()

from deleter.utils.hubspot_client import HubspotClient
from deleter.core import (
    initialize_deleter,
    delete_all_records,
    delete_objects_by_type,
    delete_by_date_criteria,
    delete_by_query as core_delete_by_query,
    delete_from_csv as core_delete_from_csv,
    delete_by_property as core_delete_by_property
)

# Initialize without arguments
initialize_deleter()

@click.group()
def cli():
    """HubSpot Record Deletion Tool
    
    This tool allows you to delete records from a HubSpot portal based on various criteria.
    """
    logger.info("Starting HubSpot Record Deletion Tool")


@cli.command(name="delete-all")
def delete_all():
    """Delete all records in a HubSpot portal."""
    logger.info("Deleting all records")
    
    try:
        result = delete_all_records()
        logger.info(f"Deletion complete. Results: {result}")
        click.echo(f"Successfully deleted records. Summary: {result}")
    except Exception as e:
        logger.error(f"Error during deletion: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)


@cli.command(name="delete-objects")
@click.argument('object-types', nargs=-1, required=True)
def delete_objects(object_types):
    """Delete all records of specified object type(s)."""
    logger.info(f"Deleting objects of types: {', '.join(object_types)}")
    
    results = {}
    try:
        for object_type in object_types:
            count = delete_objects_by_type(object_type)
            results[object_type] = count
            
        logger.info(f"Deletion complete. Results: {results}")
        click.echo(f"Successfully deleted records. Summary: {results}")
    except Exception as e:
        logger.error(f"Error during deletion: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)


@cli.command(name="delete-by-date")
@click.option('--created-after', 
              type=click.DateTime(formats=["%Y-%m-%dT%H:%M:%S%z"]),  # Require explicit timezone
              help='Delete records created after this date/time. Format: YYYY-MM-DDTHH:MM:SS±HHMM (e.g., "2024-03-01T00:00:00-0500" or "2024-03-01T00:00:00+0000")')
@click.option('--modified-after', 
              type=click.DateTime(formats=["%Y-%m-%dT%H:%M:%S%z"]),  # Require explicit timezone
              help='Delete records modified after this date/time. Format: YYYY-MM-DDTHH:MM:SS±HHMM (e.g., "2024-03-01T00:00:00-0500" or "2024-03-01T00:00:00+0000")')
@click.option('--object-types',
              multiple=True,
              type=click.Choice(['contacts', 'companies', 'deals', 'tickets', 'notes', 'emails', 'tasks', 'meetings', 'calls', 'all']),
              default=['all'],
              help='Object types to process. Use --object-types multiple times for multiple types, or use "all" for all types.')
def delete_by_date(created_after, modified_after, object_types):
    """Delete records based on creation or modification date.
    
    Dates must be provided in ISO 8601 format with timezone offset.
    The timezone offset is required to ensure accurate timestamp comparison.
    HubSpot stores all timestamps in UTC.
    
    Examples:
      - delete-by-date --created-after "2024-03-01T00:00:00-0500"  # Eastern Time
      - delete-by-date --modified-after "2024-03-01T05:00:00+0000" # UTC
      - delete-by-date --created-after "2024-03-01T00:00:00+0000" --object-types contacts --object-types companies
    """
    if not created_after and not modified_after:
        logger.error("Either --created-after or --modified-after must be specified")
        click.echo("Error: Either --created-after or --modified-after must be specified", err=True)
        return
    
    try:
        # If 'all' is in the list, process all object types
        process_all = 'all' in object_types
        object_types_to_process = None if process_all else list(object_types)
        
        result = delete_by_date_criteria(created_after, modified_after, object_types_to_process)
        logger.info(f"Deletion complete. Results: {result}")
        click.echo(f"Successfully deleted records. Summary: {result}")
    except Exception as e:
        logger.error(f"Error during deletion: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)


@cli.command(name="delete-by-query")
@click.argument('object-type', required=True)
@click.argument('query', required=True)
def delete_by_query(object_type, query):
    """Delete records based on a HubSpot search API query."""
    logger.info(f"Deleting {object_type} records matching query: {query}")
    
    try:
        count = core_delete_by_query(object_type, query)
        logger.info(f"Deletion complete. Deleted {count} records")
        click.echo(f"Successfully deleted {count} records of type {object_type}")
    except Exception as e:
        logger.error(f"Error during deletion: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)


@cli.command(name="delete-from-csv")
@click.argument('object-type', required=True, 
                type=click.Choice(['contacts', 'companies', 'deals', 'tickets', 'notes', 'emails', 'tasks', 'meetings', 'calls']))
@click.argument('csv-file', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option('--id-column', default='hubspot_id', help='Column name containing HubSpot IDs')
def delete_from_csv(object_type, csv_file, id_column):
    """Delete records of a specific object type listed in a CSV file."""
    logger.info(f"Deleting {object_type} records from CSV file: {csv_file} (ID column: {id_column})")
    
    try:
        count = core_delete_from_csv(object_type, csv_file, id_column)
        logger.info(f"Deletion complete. Deleted {count} {object_type}")
        click.echo(f"Successfully deleted {count} {object_type}")
    except Exception as e:
        logger.error(f"Error during deletion: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)


@cli.command(name="delete-by-property")
@click.argument('object-type', required=True, 
                type=click.Choice(['contacts', 'companies', 'deals', 'tickets', 'notes', 'emails', 'tasks', 'meetings', 'calls']))
@click.argument('property-name', required=True)
def delete_by_property(object_type, property_name):
    """Delete records that have a specific property set."""
    logger.info(f"Deleting {object_type} records with property: {property_name}")
    
    try:
        count = core_delete_by_property(object_type, property_name)
        logger.info(f"Deletion complete. Deleted {count} records")
        click.echo(f"Successfully deleted {count} {object_type} records")
    except Exception as e:
        logger.error(f"Error during deletion: {str(e)}")
        click.echo(f"Error: {str(e)}", err=True)


if __name__ == '__main__':
    cli()
