import csv
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Union
import time

from dotenv import load_dotenv

from deleter.utils.logger import get_logger
from deleter.utils.hubspot_client import HubspotClient, HubspotError

load_dotenv()


class HubSpotDeleter:
    """
    Main class for handling HubSpot record deletion operations.
    """
    
    def __init__(self):
        self.token = os.environ.get("HUBSPOT_TOKEN")
        self.client = HubspotClient(self.token)
        self.logger = get_logger()
    
    def delete_all_records(self) -> Dict[str, int]:
        """
        Delete all records in a HubSpot portal.
        
        Returns:
            Dictionary with object types as keys and count of deleted records as values
        """
        self.logger.info("Starting deletion of all records")
        
        # Combine all object types
        all_object_types = self._get_all_object_types() + self._get_custom_object_types()
        
        deletion_counts = {}
        
        # Delete records for each object type
        for object_type in all_object_types:
            count = self.delete_objects_by_type(object_type)
            deletion_counts[object_type] = count
        
        self.logger.info(f"Completed deletion of all records. Summary: {deletion_counts}")
        return deletion_counts
    
    def delete_objects_by_type(self, object_type: str) -> int:
        """
        Delete all records of a specific object type.
        
        Args:
            object_type: The HubSpot object type to delete
            
        Returns:
            Count of deleted records
        """
        self.logger.info(f"Deleting all records of type: {object_type}")
        
        # Get all records of this type
        records = self._get_all_records_by_type(object_type)
        
        # Delete the records
        deleted_count = self._delete_records(object_type, records)
        
        self.logger.info(f"Deleted {deleted_count} records of type {object_type}")
        return deleted_count
    
    def delete_by_date_criteria(self, created_after=None, modified_after=None, object_types=None) -> Dict[str, int]:
        """Delete records based on creation or modification date.
        
        Args:
            created_after: Delete records created after this datetime
            modified_after: Delete records modified after this datetime
            object_types: List of object types to process. If None, processes all types.
            
        Returns:
            Dictionary with object types as keys and count of deleted records as values
        """
        self.logger.info(f"Deleting records by date criteria - created after: {created_after}, modified after: {modified_after}")
        
        # Get all object types
        all_object_types = self._get_all_object_types() + self._get_custom_object_types()
        
        # If object_types is None, use all object types
        if object_types is None:
            object_types = all_object_types
        
        deletion_counts = {}
        
        # For each object type, get records matching the date criteria and delete them
        for object_type in object_types:
            records = self._get_records_by_date_criteria(object_type, created_after, modified_after)
            count = self._delete_records(object_type, records)
            deletion_counts[object_type] = count
        
        self.logger.info(f"Completed deletion by date criteria. Summary: {deletion_counts}")
        return deletion_counts
    
    def delete_by_query(self, object_type: str, query: str) -> int:
        """
        Delete records based on a HubSpot search API query.
        
        Args:
            object_type: The HubSpot object type to query
            query: The search query to use
            
        Returns:
            Count of deleted records
        """
        self.logger.info(f"Deleting {object_type} records matching query: {query}")
        
        # Execute the search query
        records = self._search_records(object_type, query)
        
        # Delete the matching records
        deleted_count = self._delete_records(object_type, records)
        
        self.logger.info(f"Deleted {deleted_count} {object_type} records matching query")
        return deleted_count

    def delete_from_csv(self, object_type: str, csv_file: str, id_column: str) -> int:
        """Internal method to delete records of a specific type from a CSV file."""
        self.logger.debug(f"Reading {object_type} records from CSV file: {csv_file}")
        
        records = []
        try:
            with open(csv_file, 'r', newline='') as file:
                reader = csv.DictReader(file)
                
                # Ensure the ID column exists
                if id_column not in reader.fieldnames:
                    raise ValueError(f"Column '{id_column}' not found in CSV file")
                
                for row in reader:
                    if row[id_column]:  # Skip empty IDs
                        records.append({'id': row[id_column]})
                
                self.logger.info(f"Found {len(records)} {object_type} records in CSV file")
                
                # Use the existing _delete_records method to perform the deletion
                return self._delete_records(object_type, records)
        
        except Exception as e:
            self.logger.error(f"Error processing CSV file: {e}")
            raise


    def delete_by_property(self, object_type: str, property_name: str) -> int:
        """Delete all records that have a specific property set."""
        self.logger.info(f"Deleting {object_type} records with property: {property_name}")
        
        filters = [self._build_property_exists_filter(property_name)]
        records = self._search_records(object_type, filters)
        
        return self._delete_records(object_type, records)    
    
    # Helper methods
    
    def _get_all_object_types(self) -> List[str]:
        """Get all standard HubSpot object types."""
        # This would typically call the HubSpot API to get object types
        # For now, return a hardcoded list of common types
        return ['contacts', 'companies', 'deals', 'tickets', 'notes', 'emails', 'tasks', 'meetings', 'calls']
    
    def _get_custom_object_types(self) -> List[str]:
        """Get all custom object types in the portal."""
        # This would call the HubSpot API to get custom object schemas
        # For now, return an empty list
        return []
    
    def _get_all_records_by_type(self, object_type: str) -> List[Dict[str, Any]]:
        self.logger.debug(f"Fetching all records of type: {object_type}")
        
        all_records = []
        page_size = 100
        after = None
        
        try:
            while True:
                self.logger.debug(f"Fetching page of {object_type} records" + 
                                 (f" after {after}" if after else ""))
                
                endpoint = f"/crm/v3/objects/{object_type}"
                params = {
                    "limit": page_size,
                }
                
                if after:
                    params["after"] = after
                
                response = self.client.get(endpoint, params=params)
                records = response.get("results", [])
                
                for record in records:
                    all_records.append({"id": record.get("id")})
                
                pagination = response.get("paging", {})
                next_page = pagination.get("next", {})
                after = next_page.get("after")
                
                if not after:
                    break
                
                self.logger.debug(f"Retrieved {len(all_records)} {object_type} records so far")
        
        except HubspotError as e:
            self.logger.error(f"Error fetching {object_type} records: {str(e)}")
        
        self.logger.info(f"Retrieved a total of {len(all_records)} {object_type} records")
        return all_records
    
    def _get_records_by_date_criteria(self, 
                                     object_type: str, 
                                     created_after: Optional[datetime], 
                                     modified_after: Optional[datetime]) -> List[Dict[str, Any]]:
        """Get records matching date criteria using the search API."""
        filters = []
        
        if created_after:
            filters.append(self._build_date_filter("createdate", created_after))
        
        if modified_after:
            filters.append(self._build_date_filter("lastmodifieddate", modified_after))
        
        if not filters:
            return []
        return self._search_records(object_type, filters)
    
    def _search_records(self, object_type: str, filters: List[dict]) -> List[Dict[str, Any]]:
        """Search for records using the HubSpot search API."""
        self.logger.debug(f"Searching for {object_type} records with filters: {filters}")
        
        all_records = []
        last_record_id = 0
        
        while True:
            # Sleep between new search queries to avoid rate limits
            if last_record_id > 0:
                self.logger.debug(f"Starting new search batch with ID threshold > {last_record_id}")
                time.sleep(1)
            
            current_filters = self._add_id_threshold_filter(filters, last_record_id)
            search_payload = self._create_search_payload(current_filters)
            self.logger.debug(f"Search payload: {search_payload}")
            
            try:
                response = self.client.post(f"/crm/v3/objects/{object_type}/search", search_payload)
                total = response.get("total", 0)
                
                if total == 0:
                    break
                    
                self.logger.info(f"Found {total} {object_type} records with ID > {last_record_id}")
                
                # Store the search payload in the response for pagination
                response["_request_payload"] = search_payload
                
                # Process ALL pages of this search before potentially starting a new one
                batch_records, last_id = self._process_search_batch(object_type, response)
                all_records.extend(batch_records)
                last_record_id = int(last_id)
                
                # If total was less than 10000, we're done
                if total < 10000:
                    break
                    
                self.logger.info(
                    f"Completed processing {len(batch_records)} records, next search will start with ID > {last_record_id}"
                )
                
            except HubspotError as e:
                self.logger.error(f"Error searching {object_type} records: {str(e)}")
                raise
        
        self.logger.info(f"Found total of {len(all_records)} matching {object_type} records")
        return all_records

    def _add_id_threshold_filter(self, base_filters: List[dict], threshold_id: int) -> List[dict]:
        """Add ID threshold filter to base filters."""
        current_filters = base_filters.copy()
        id_filter = {
            "propertyName": "hs_object_id",
            "operator": "GT",
            "value": str(threshold_id)
        }
        current_filters.append(id_filter)
        self.logger.debug(f"Added ID threshold filter: {id_filter}")
        return current_filters

    def _create_search_payload(self, filters: List[dict], limit: int = 200) -> dict:
        """Create the search API payload."""
        return {
            "filterGroups": [{"filters": filters}],
            "sorts": [
                {
                    "propertyName": "hs_object_id",
                    "direction": "ASCENDING"
                }
            ],
            "limit": limit
        }

    def _process_search_batch(self, object_type: str, initial_response: dict) -> tuple[List[dict], int]:
        """Process a batch of search results, including pagination if needed."""
        batch_records = []
        last_id = 0
        total = initial_response.get("total", 0)
        
        # Process initial page
        results = initial_response.get("results", [])
        if results:
            self.logger.debug(f"Initial page - First record ID: {results[0].get('id')}, Last record ID: {results[-1].get('id')}")
        
        for record in results:
            record_id = record.get("id")
            batch_records.append({"id": record_id})
            last_id = int(record_id)
        
        # Handle pagination if there are more pages
        after = initial_response.get("paging", {}).get("next", {}).get("after")
        while after:
            # Small sleep between pagination requests
            time.sleep(0.1)
            
            # Important: Use the same filters as the initial request
            payload = initial_response.get("_request_payload", {})  # We'll need to pass this from _search_records
            payload["after"] = after
            
            response = self.client.post(
                f"/crm/v3/objects/{object_type}/search", 
                payload
            )
            
            results = response.get("results", [])
            if results:
                self.logger.debug(
                    f"Page results - First record ID: {results[0].get('id')}, "
                    f"Last record ID: {results[-1].get('id')}, "
                    f"Current last_id: {last_id}"
                )
            
            for record in results:
                record_id = record.get("id")
                batch_records.append({"id": record_id})
                last_id = int(record_id)
            
            after = response.get("paging", {}).get("next", {}).get("after")
                        
            # Stop if we've hit 10000 even if there's an after token
            if len(batch_records) >= 10000:
                self.logger.info("Reached 10000 record limit, stopping pagination regardless of after token")
                break

            self.logger.debug(f"Retrieved {len(batch_records)}/{total} {object_type} records in current batch")
        
        self.logger.info(f"Completed batch processing with {len(batch_records)} records. Final last_id: {last_id}")
        return batch_records, last_id
    
    def _build_date_filter(self, property_name: str, timestamp: datetime) -> dict:
        """Build a date filter for the search API.
        
        Args:
            property_name: The HubSpot property name (createdate or lastmodifieddate)
            timestamp: The datetime with timezone information
            
        Returns:
            Dict containing the filter for HubSpot's search API
        """
        if timestamp.tzinfo is None:
            raise ValueError(
                f"Timestamp must include timezone information. "
                f"Received: {timestamp}. "
                f"Please provide in format: YYYY-MM-DDTHH:MM:SS±HHMM"
            )
        
        # Convert to UTC timestamp in milliseconds
        utc_timestamp = int(timestamp.astimezone(timezone.utc).timestamp() * 1000)
        
        self.logger.debug(
            f"Converting {timestamp} ({timestamp.tzinfo}) to UTC milliseconds: {utc_timestamp}"
        )
        
        return {
            "propertyName": property_name,
            "operator": "GTE",
            "value": str(utc_timestamp)
        }
    
    def _build_property_exists_filter(self, property_name: str) -> dict:
        """Build a filter to find records where a property has any value."""
        return {
            "propertyName": property_name,
            "operator": "HAS_PROPERTY"
        }
    
    def _read_csv_records(self, csv_file: str, id_column: str) -> Dict[str, List[Dict[str, Any]]]:
        """Read records from a CSV file and organize by object type."""
        self.logger.debug(f"Reading records from CSV file: {csv_file}")
        
        records_by_type = {}
        
        try:
            with open(csv_file, 'r', newline='') as file:
                reader = csv.DictReader(file)
                
                # Ensure the ID column exists
                if id_column not in reader.fieldnames:
                    raise ValueError(f"Column '{id_column}' not found in CSV file")
                
                # If there's an object_type column, use it to categorize records
                has_object_type = 'object_type' in reader.fieldnames
                
                for row in reader:
                    record_id = row[id_column]
                    if not record_id:
                        continue
                    
                    # Determine object type
                    if has_object_type and row['object_type']:
                        object_type = row['object_type']
                    else:
                        # Default to 'contacts' if no object type specified
                        object_type = 'contacts'
                    
                    # Initialize the list for this object type if needed
                    if object_type not in records_by_type:
                        records_by_type[object_type] = []
                    
                    # Add the record
                    records_by_type[object_type].append({'id': record_id})
        
        except Exception as e:
            self.logger.error(f"Error reading CSV file: {e}")
            raise
        
        return records_by_type
    
    def _delete_records(self, 
                       object_type: str, 
                       records: List[Dict[str, Any]]) -> int:
        self.logger.debug(f"Deleting {len(records)} records of type {object_type}")
        
        deleted_count = 0
        batch_size = 100  # HubSpot's maximum batch size
        
        # Process records in batches
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            batch_ids = [{"id": record['id']} for record in batch]
            
            try:
                # Use the batch archive endpoint
                endpoint = f"/crm/v3/objects/{object_type}/batch/archive"
                payload = {"inputs": batch_ids}
                
                self.client.post(endpoint, payload)
                deleted_count += len(batch)
                self.logger.info(f"Deleted batch of {len(batch)} {object_type} records ({deleted_count}/{len(records)} total)")
                
            except HubspotError as e:
                self.logger.error(f"Error deleting batch of {object_type} records: {e}")
                # If batch fails, try deleting records individually
                self.logger.info(f"Attempting to delete records individually after batch failure")
                for record in batch:
                    try:
                        individual_endpoint = f"/crm/v3/objects/{object_type}/{record['id']}"
                        self.client.delete(individual_endpoint)
                        deleted_count += 1
                    except Exception as individual_error:
                        self.logger.error(f"Error deleting individual {object_type} record {record['id']}: {individual_error}")
        
        self.logger.info(f"Successfully deleted {deleted_count} out of {len(records)} {object_type} records")
        return deleted_count
    
    def _delete_associated_engagements(self, object_type: str, record_id: str) -> int:
        """Delete engagements associated with a specific record."""
        # This would call the HubSpot API to get and delete associated engagements
        self.logger.debug(f"Deleting engagements for {object_type} record {record_id}")
        # Placeholder implementation
        return 0

# Convenience functions that use a singleton HubSpotDeleter instance

_deleter_instance = None

def initialize_deleter():
    """Initialize the global deleter instance."""
    global _deleter_instance
    _deleter_instance = HubSpotDeleter()
    return _deleter_instance

def get_deleter():
    """Get the global deleter instance."""
    if _deleter_instance is None:
        raise RuntimeError("Deleter not initialized. Call initialize_deleter first.")
    return _deleter_instance

def delete_all_records() -> Dict[str, int]:
    """Delete all records in the HubSpot portal."""
    return get_deleter().delete_all_records()

def delete_objects_by_type(object_type: str) -> int:
    """Delete all records of a specific object type."""
    return get_deleter().delete_objects_by_type(object_type)

def delete_by_date_criteria(created_after=None, modified_after=None, object_types=None):
    """Delete records based on creation or modification date.
    
    Args:
        created_after: Delete records created after this datetime
        modified_after: Delete records modified after this datetime
        object_types: List of object types to process. If None, processes all types.
        
    Returns:
        Dictionary with object types as keys and count of deleted records as values
    """
    return get_deleter().delete_by_date_criteria(created_after, modified_after, object_types)

def delete_by_query(object_type, query):
    """Delete records based on a HubSpot search API query."""
    return get_deleter().delete_by_query(object_type, query)

def delete_from_csv(object_type, csv_file, id_column='hubspot_id'):
    """Delete records listed in a CSV file."""
    return get_deleter().delete_from_csv(object_type, csv_file, id_column)

def delete_by_property(object_type: str, property_name: str) -> int:
    """Delete all records that have a specific property set."""
    return get_deleter().delete_by_property(object_type, property_name)
