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

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagBag, DAG
from google.cloud import dataplex_v1
from google.protobuf import struct_pb2
import logging

# Set up logger
logger = logging.getLogger(__name__)

project_id = "my-project-id"
location = "us-central1"
entry_group_id = "composer-dag-test"
entry_type = "airflow-dag"
aspect_type = "dag-metadata"

def create_entry(project_id: str, location: str, entry_group_id: str, entry_id: str, entry_type: str, display_name: str, dag_id: str, dag: DAG, aspect_type: str):
    """
    Create an Entry in Dataplex API using the dataplex_v1 client.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The location of the Dataplex resource.
        entry_group_id (str): The ID of the entry group to create the entry in.
        entry_id (str): The ID of the entry to create.
        entry_type (str): The type of the entry.
        display_name (str): The display name of the entry.
        dag_id (str): The ID of the DAG.
        dag (DAG): The Airflow DAG object.

    Raises:
        Exception: If there's an error creating the entry.
    """
    logger.info(f"Creating entry for DAG: {entry_id}")
    client = dataplex_v1.CatalogServiceClient()
    parent = client.entry_group_path(project_id, location, entry_group_id)
    logger.debug(f"Parent path: {parent}")

    # Create the entry object
    entry = dataplex_v1.Entry()
    entry.entry_type = f"projects/{project_id}/locations/{location}/entryTypes/{entry_type}"
    entry.name = display_name

    # Create the aspect object with DAG metadata
    aspect = dataplex_v1.Aspect()
    aspect.aspect_type = aspect_type
    aspect_content = {
        "dag-id": dag.dag_id,
        "description": dag.dag_display_name,
        "schedule": str(dag.schedule_interval),
        "start-date": str(dag.start_date),
        "end-date": str(dag.end_date),
        "owner": dag.owner
    }
    data_struct = struct_pb2.Struct()
    data_struct.update(aspect_content)
    aspect.data = data_struct
    entry.aspects[f"{project_id}.{location}.{aspect.aspect_type}"] = aspect

    # Create the entry request
    request = dataplex_v1.CreateEntryRequest(
        parent=parent,
        entry_id=entry_id,
        entry=entry,
    )

    try:
        operation = client.create_entry(request=request)
        logger.info(f"Successfully created entry for DAG: {entry_id}")
        logger.debug(f"Operation details: {operation}")
    except Exception as e:
        logger.error(f"Error creating entry for DAG {entry_id}: {str(e)}")
        raise

def export_dags_to_dataplex(**kwargs):
    """
    Export all DAGs in the Airflow environment to Dataplex.

    This function retrieves all DAGs from the Airflow environment and creates
    corresponding entries in Dataplex for each DAG.

    Args:
        **kwargs: Keyword arguments passed by Airflow (not used in this function).
    """

    logger.info("Starting export of DAGs to Dataplex")
    dag_bag = DagBag()
    logger.info(f"Found {len(dag_bag.dags)} DAGs in the Airflow environment")

    for dag_id, dag in dag_bag.dags.items():
        logger.info(f"Processing DAG: {dag_id}")

        try:
            create_entry(
                project_id=project_id,
                location=location,
                entry_group_id=entry_group_id,
                entry_id=dag_id,
                entry_type=entry_type,
                display_name=dag.description or f"Airflow DAG: {dag_id}",
                dag_id=dag_id,
                dag=dag,
                aspect_type=aspect_type
            )
        except Exception as e:
            logger.error(f"Failed to create entry for DAG {dag_id}: {str(e)}")

    logger.info("Finished exporting DAGs to Dataplex")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='dataplex_export_dags',
    default_args=default_args,
    description='Dataplex Export Airflow DAGs to Catalog',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Define the export task
    export_task = PythonOperator(
        task_id='export_dags_to_dataplex',
        python_callable=export_dags_to_dataplex,
    )

    # Set the task dependencies (in this case, there's only one task)
    export_task