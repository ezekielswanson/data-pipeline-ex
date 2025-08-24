from typing import Dict, Any, Optional, List
from urllib.parse import urljoin

import requests

from migrator.utils.logger import get_logger

logger = get_logger()

class HubspotError(Exception):
    """Base exception for HubSpot API errors."""
    pass

class HubspotClientError(HubspotError):
    """4xx client errors from HubSpot API."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"HubSpot API client error: {status_code} - {message}")

class HubspotServerError(HubspotError):
    """5xx server errors from HubSpot API."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"HubSpot API server error: {status_code} - {message}")

class HubspotRateLimitError(HubspotClientError):
    """429 rate limit errors from HubSpot API."""
    def __init__(self, retry_after: Optional[int] = None):
        self.retry_after = retry_after
        super().__init__(429, "Rate limit exceeded")

class HubspotDuplicateError(HubspotClientError):
    """409 conflict errors from HubSpot API."""
    def __init__(self, message: str, duplicate_info: Optional[Dict[str, Any]] = None):
        self.duplicate_info = duplicate_info or {}
        super().__init__(409, message)

class HubspotClient:
    """Base client for interacting with the HubSpot API."""
    
    BASE_URL = "https://api.hubapi.com"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        })

    def _handle_error_response(self, response: requests.Response) -> None:
        """Handle error responses from HubSpot API."""
        try:
            error_data = response.json()
            message = error_data.get('message', response.text)
        except ValueError:
            message = response.text
            error_data = {}

        if response.status_code == 429:
            retry_after = response.headers.get('Retry-After')
            raise HubspotRateLimitError(retry_after=int(retry_after) if retry_after else None)
        elif response.status_code == 409:
            # Extract duplicate information from the error response
            duplicate_info = error_data.get('duplicateProperties', {})
            raise HubspotDuplicateError(message, duplicate_info)
        elif 400 <= response.status_code < 500:
            raise HubspotClientError(response.status_code, message)
        elif response.status_code >= 500:
            raise HubspotServerError(response.status_code, message)
    
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a GET request to the HubSpot API."""
        url = urljoin(self.BASE_URL, endpoint)
        try:
            response = self.session.get(url, params=params)
            if response.ok:
                return response.json()
            self._handle_error_response(response)
        except requests.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise HubspotError(f"Request failed: {str(e)}")
    
    def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        url = urljoin(self.BASE_URL, endpoint)
        try:
            response = self.session.post(url, json=data)
            if response.ok:
                return response.json()
            self._handle_error_response(response)
        except requests.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise HubspotError(f"Request failed: {str(e)}")
    
    def put(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        url = urljoin(self.BASE_URL, endpoint)
        try:
            response = self.session.put(url, json=data)
            if response.ok:
                return response.json()
            self._handle_error_response(response)
        except requests.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise HubspotError(f"Request failed: {str(e)}")
    
    def delete(self, endpoint: str) -> None:
        url = urljoin(self.BASE_URL, endpoint)
        try:
            response = self.session.delete(url)
            if response.ok:
                return
            self._handle_error_response(response)
        except requests.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise HubspotError(f"Request failed: {str(e)}")