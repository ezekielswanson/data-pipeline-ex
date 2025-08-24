import pytest
import responses
from requests.exceptions import RequestException

from migrator.utils.hubspot_client import (
    HubspotClient,
    HubspotError,
    HubspotClientError,
    HubspotServerError,
    HubspotRateLimitError,
    HubspotDuplicateError
)

@pytest.fixture
def client():
    """Create a test HubspotClient instance."""
    return HubspotClient("test-api-key")

@pytest.fixture
def base_url():
    """Return the base URL for HubSpot API."""
    return HubspotClient.BASE_URL

class TestHubspotClient:
    @responses.activate
    def test_successful_get_request(self, client, base_url):
        """Test successful GET request."""
        # Arrange
        endpoint = "/crm/v3/objects/contacts"
        mock_response = {"results": [{"id": "1", "properties": {"email": "test@example.com"}}]}
        responses.add(
            responses.GET,
            f"{base_url}{endpoint}",
            json=mock_response,
            status=200
        )

        # Act
        response = client.get(endpoint)

        # Assert
        assert response == mock_response
        assert len(responses.calls) == 1
        assert responses.calls[0].request.headers["Authorization"] == "Bearer test-api-key"

    @responses.activate
    def test_rate_limit_error(self, client, base_url):
        """Test rate limit (429) handling."""
        # Arrange
        endpoint = "/crm/v3/objects/contacts"
        responses.add(
            responses.GET,
            f"{base_url}{endpoint}",
            json={"message": "Rate limit exceeded"},
            status=429,
            headers={"Retry-After": "60"}
        )

        # Act & Assert
        with pytest.raises(HubspotRateLimitError) as exc_info:
            client.get(endpoint)
        
        assert exc_info.value.retry_after == 60

    @responses.activate
    def test_duplicate_error(self, client, base_url):
        """Test duplicate (409) handling."""
        # Arrange
        endpoint = "/crm/v3/objects/contacts"
        # TODO: See what the response should be
        mock_response = {
            "message": "Contact already exists",
            "duplicateProperties": {
                "email": "existing@example.com"
            }
        }
        responses.add(
            responses.POST,
            f"{base_url}{endpoint}",
            json=mock_response,
            status=409
        )

        # Act & Assert
        with pytest.raises(HubspotDuplicateError) as exc_info:
            client.post(endpoint, {"properties": {"email": "existing@example.com"}})
        
        assert exc_info.value.status_code == 409
        assert exc_info.value.duplicate_info == {"email": "existing@example.com"}

    @responses.activate
    def test_client_error(self, client, base_url):
        """Test client error (4xx) handling."""
        # Arrange
        endpoint = "/crm/v3/objects/contacts"
        responses.add(
            responses.GET,
            f"{base_url}{endpoint}",
            json={"message": "Invalid request"},
            status=400
        )

        # Act & Assert
        with pytest.raises(HubspotClientError) as exc_info:
            client.get(endpoint)
        
        assert exc_info.value.status_code == 400
        assert "Invalid request" in str(exc_info.value)

    @responses.activate
    def test_server_error(self, client, base_url):
        """Test server error (5xx) handling."""
        # Arrange
        endpoint = "/crm/v3/objects/contacts"
        responses.add(
            responses.GET,
            f"{base_url}{endpoint}",
            json={"message": "Internal server error"},
            status=500
        )

        # Act & Assert
        with pytest.raises(HubspotServerError) as exc_info:
            client.get(endpoint)
        
        assert exc_info.value.status_code == 500
        assert "Internal server error" in str(exc_info.value)

    @responses.activate
    def test_network_error(self, client, base_url):
        """Test network error handling."""
        # Arrange
        endpoint = "/crm/v3/objects/contacts"
        responses.add(
            responses.GET,
            f"{base_url}{endpoint}",
            body=RequestException("Connection error")
        )

        # Act & Assert
        with pytest.raises(HubspotError) as exc_info:
            client.get(endpoint)
        
        assert "Connection error" in str(exc_info.value)

    @responses.activate
    def test_successful_post_request(self, client, base_url):
        """Test successful POST request."""
        # Arrange
        endpoint = "/crm/v3/objects/contacts"
        mock_response = {"id": "1", "properties": {"email": "new@example.com"}}
        responses.add(
            responses.POST,
            f"{base_url}{endpoint}",
            json=mock_response,
            status=200
        )

        # Act
        response = client.post(endpoint, {"properties": {"email": "new@example.com"}})

        # Assert
        assert response == mock_response
        assert len(responses.calls) == 1
        assert responses.calls[0].request.headers["Content-Type"] == "application/json"

    @responses.activate
    def test_successful_put_request(self, client, base_url):
        """Test successful PUT request."""
        # Arrange
        endpoint = "/crm/v3/objects/contacts/1"
        mock_response = {"id": "1", "properties": {"email": "updated@example.com"}}
        responses.add(
            responses.PUT,
            f"{base_url}{endpoint}",
            json=mock_response,
            status=200
        )

        # Act
        response = client.put(endpoint, {"properties": {"email": "updated@example.com"}})

        # Assert
        assert response == mock_response

    @responses.activate
    def test_successful_delete_request(self, client, base_url):
        """Test successful DELETE request."""
        # Arrange
        endpoint = "/crm/v3/objects/contacts/1"
        responses.add(
            responses.DELETE,
            f"{base_url}{endpoint}",
            status=204
        )

        # Act & Assert
        client.delete(endpoint) 
        assert len(responses.calls) == 1