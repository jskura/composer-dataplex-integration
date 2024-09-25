"""
Copyright 2024 Jakub Skuratowicz

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from google.cloud import dataplex_v1
import logging

logger = logging.getLogger(__name__)

project_id="my-project-id"
entry_type_id="airflow-dag"
aspect_type_id="dag-metadata"
entry_group_id="composer-dag-test"


def sample_create_entry_type(project_id: str, location: str, entry_type_id: str):
    """
    Create a new Aspect Type in Google Cloud Dataplex.

    This function creates a new Aspect Type with metadata template for DAG (Directed Acyclic Graph) information.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The location where the Aspect Type will be created.
        aspect_type_id (str): The ID for the new Aspect Type.

    Raises:
        Exception: If the creation of the Aspect Type fails.
    """
    # Create a client
    client = dataplex_v1.CatalogServiceClient()

   
    # Create the Aspect Type
    entry_type = dataplex_v1.EntryType()


    entry_type.name = entry_type_id
    entry_type.description = "Airflow DAGs run in Cloud Composer"
    entry_type.system = "Cloud Composer"
    entry_type.display_name = "Airflow DAGs"
    entry_type.platform = "Google Cloud"

    # Prepare the request
    request = dataplex_v1.CreateEntryTypeRequest(
        parent=f"projects/{project_id}/locations/{location}",
        entry_type=entry_type,
        entry_type_id=entry_type_id
    )

    # Make the request
    try:
        operation = client.create_entry_type(request=request)
        print("Waiting for operation to complete...")
        response = operation.result()
        # Handle the response
        print(response)
    except Exception as e:
        
        logger.error(f"Failed to create aspect type: {e}")

def sample_create_aspect_type(project_id: str, location: str, aspect_type_id: str):
    """
    Create a new Aspect Type in Google Cloud Dataplex.

    This function creates a new Aspect Type with metadata template for DAG (Directed Acyclic Graph) information.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The location where the Aspect Type will be created.
        aspect_type_id (str): The ID for the new Aspect Type.

    Raises:
        Exception: If the creation of the Aspect Type fails.
    """
    # Create a client
    client = dataplex_v1.CatalogServiceClient()

    # Initialize request argument(s)
    aspect_type = dataplex_v1.AspectType()
    aspect_type.metadata_template.name = "DagMetadata"
    aspect_type.metadata_template.type_ = "record"

   
    metadata_template = {
        "type_": "record",
        "name": "DagMetadata",
        "record_fields": [
            {
                "type_": "string",
                "name": "dag-id",
                "index": 1,
                "annotations": {
                    "description": "Dag ID",
                    "display_name": "Dag ID"
                }
            },
            {
                "type_": "string",
                "name": "description",
                "index": 2,
                "annotations": {
                    "description": "Dag description",
                    "display_name": "Description"
                }
            },
            {
                "type_": "string",
                "name": "schedule",
                "index": 3,
                "annotations": {
                    "description": "Dag schedule",
                    "display_name": "Schedule",                    
                }
            },
            {
                "type_": "string",
                "name": "start-date",
                "index": 4,
                "annotations": {
                    "description": "Date when the dag is scheduled to start",
                    "display_name": "Start Date"
                }
            },
            {
                "type_": "string",
                "name": "end-date",
                "index": 5,
                "annotations": {
                    "description": "Indicates until when the dag is active",
                    "display_name": "Active"
                }
            },
            {
                "type_": "string",
                "name": "owner",
                "index": 6,
                "annotations": {
                    "description": "Dag owner",
                    "display_name": "Owner"
                }
            },
        ]
    }

    # Create the Aspect Type
    aspect_type = dataplex_v1.AspectType()
    aspect_type.metadata_template = metadata_template

    # Prepare the request
    request = dataplex_v1.CreateAspectTypeRequest(
        parent=f"projects/{project_id}/locations/{location}",
        aspect_type_id=aspect_type_id,
        aspect_type=aspect_type,
    )

    # Make the request
    try:
        operation = client.create_aspect_type(request=request)
        print("Waiting for operation to complete...")
        response = operation.result()
        # Handle the response
        print(response)
    except Exception as e:
        print(f"Failed to create aspect type: {e}")
        logger.error(f"Failed to create aspect type: {e}")

def sample_create_entry_group(project_id: str, location: str, entry_group_id: str):
    """
    Create a new Entry Group in Google Cloud Dataplex.

    This function creates a new Entry Group with metadata template for DAG (Directed Acyclic Graph) information.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The location where the Entry Group will be created.
    # Example usage
    """
    # Create a client
    client = dataplex_v1.CatalogServiceClient()

    # Initialize request argument(s)
    entry_group = dataplex_v1.EntryGroup()
    entry_group.display_name = "Cloud Composer DAGs"
    entry_group.description = "Environment Cloud Composer xyz"
    entry_group.name = "Production Cloud Composer Dags"
    entry_group.labels = {"env": "prod"}

    # Prepare the request
    request = dataplex_v1.CreateEntryGroupRequest(
        parent=f"projects/{project_id}/locations/{location}",
        entry_group=entry_group,
        entry_group_id=entry_group_id
    )

    # Make the request
    try:
        operation = client.create_entry_group(request=request)
        print("Waiting for operation to complete...")
        response = operation.result()
        # Handle the response
        print(response)
    except Exception as e:
        print(f"Failed to create entry group: {e}")
        logger.error(f"Failed to create entry group: {e}")



sample_create_entry_type(project_id=project_id, location="us-central1", entry_type_id=entry_type_id)
sample_create_entry_group(project_id=project_id, location="us-central1", entry_group_id=entry_group_id)
sample_create_aspect_type(project_id=project_id, location="us-central1", aspect_type_id=aspect_type_id)

