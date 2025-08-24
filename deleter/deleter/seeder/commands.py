import os

import click
from dotenv import load_dotenv
from faker import Faker

from deleter.utils.hubspot_client import HubspotClient

load_dotenv()
faker = Faker()

HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN")
if not HUBSPOT_TOKEN:
    raise ValueError("HUBSPOT_TOKEN environment variable is required")

@click.group()
def cli():
    """CLI tool for seeding HubSpot with test data"""
    pass

@cli.command()
@click.option('--count', default=1, help='Number of contacts to create')
def seed_contacts(count):
    """Create test contacts in HubSpot"""
    client = HubspotClient(HUBSPOT_TOKEN)
    seeder = HubSpotSeeder(client)
    
    click.echo(f"Creating {count} contacts...")
    contacts = seeder.create_contacts(count)
    click.echo(f"Successfully created {len(contacts)} contacts")

@cli.command()
@click.option('--count', default=1, help='Number of companies to create')
def seed_companies(count):
    """Create test companies in HubSpot"""
    client = HubspotClient(HUBSPOT_TOKEN)
    seeder = HubSpotSeeder(client)
    
    click.echo(f"Creating {count} companies...")
    companies = seeder.create_companies(count)
    click.echo(f"Successfully created {len(companies)} companies")

@cli.command()
@click.option('--count', default=1, help='Number of deals to create')
def seed_deals(count):
    """Create test deals in HubSpot"""
    client = HubspotClient(HUBSPOT_TOKEN)
    seeder = HubSpotSeeder(client)
    
    click.echo(f"Creating {count} deals...")
    deals = seeder.create_deals(count)
    click.echo(f"Successfully created {len(deals)} deals")


class HubSpotSeeder:
    def __init__(self, client):
        self.client = client
        self.faker = Faker()
        self.BATCH_SIZE = 100

    def _create_batch(self, records: list[dict]) -> dict:
        """Helper method to format records for batch creation"""
        return {
            "inputs": [{"properties": props} for props in records]
        }

    def create_contacts(self, count: int) -> list[dict]:
        """Create specified number of test contacts in batches"""
        created_contacts = []
        
        # Generate all properties first
        contact_properties = [
            {"email": self.faker.email()} 
            for _ in range(count)
        ]

        # Process in batches of 100
        for i in range(0, count, self.BATCH_SIZE):
            batch = contact_properties[i:i + self.BATCH_SIZE]
            batch_payload = self._create_batch(batch)
            response = self.client.post("/crm/v3/objects/contacts/batch/create", batch_payload)
            created_contacts.extend(response.get("results", []))
        
        return created_contacts

    def create_companies(self, count: int) -> list[dict]:
        """Create specified number of test companies in batches"""
        created_companies = []
        
        company_properties = [
            {"name": self.faker.company()} 
            for _ in range(count)
        ]

        for i in range(0, count, self.BATCH_SIZE):
            batch = company_properties[i:i + self.BATCH_SIZE]
            batch_payload = self._create_batch(batch)
            response = self.client.post("/crm/v3/objects/companies/batch/create", batch_payload)
            created_companies.extend(response.get("results", []))
        
        return created_companies

    def create_deals(self, count: int) -> list[dict]:
        """Create specified number of test deals in batches"""
        created_deals = []
        
        deal_properties = [
            {
                "dealname": f"{self.faker.company()} Deal - {self.faker.bs()}",
                "pipeline": "default",
                "dealstage": "appointmentscheduled"
            }
            for _ in range(count)
        ]

        for i in range(0, count, self.BATCH_SIZE):
            batch = deal_properties[i:i + self.BATCH_SIZE]
            batch_payload = self._create_batch(batch)
            response = self.client.post("/crm/v3/objects/deals/batch/create", batch_payload)
            created_deals.extend(response.get("results", []))
        
        return created_deals

if __name__ == '__main__':
    cli()