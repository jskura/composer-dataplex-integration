# DAG Synchronization with Dataplex Catalog (Demo)

This Airflow DAG demonstrates the capability of synchronizing DAG metadata with Google Cloud Dataplex Catalog. It extracts information about DAGs and their tasks, then updates or creates corresponding entries in the Dataplex Catalog.

## Disclaimer

This is a demonstration project and is not an official Google Cloud product. The code provided is not production-ready and should be used for educational and experimental purposes only.

## Overview

The DAG performs the following main operations:

1. Retrieves a list of all DAGs in the Airflow environment.
2. For each DAG:
   - Extracts metadata (ID, description, schedule, etc.)
   - Creates or updates an entry in the Dataplex Catalog

## Prerequisites

- Google Cloud Platform account with Dataplex API enabled
- Airflow environment set up and running
- Necessary permissions to access DAG information and modify Dataplex Catalog

## Configuration

Before running the DAG, ensure the following variables are set in your Airflow environment:

- `project_id`: Your Google Cloud Project ID
- `location`: The location of your Dataplex Catalog (e.g., "us-central1")
- `entry_group_id`: The ID of the Dataplex entry group
- `entry_type`: The type of entry (e.g., "airflow-dag")
- `aspect_type`: The aspect type for DAG metadata

## DAG Structure

The DAG consists of the following main components:

1. `export_dags_to_dataplex`: A PythonOperator that processes all DAGs
   - `create_entry`: Function to create or update an entry in Dataplex Catalog
   - `export_dags_to_dataplex`: Function to iterate through all DAGs and create entries

## Usage

Please set up first Dataplex Entry Type, Entry Group and Aspect Type using create_aspect_type.py. 
Once the DAG is added to your Airflow environment and the necessary configurations are set, it will run according to the specified schedule (default is daily at 00:00).

## Key Features

- Automatic extraction of DAG metadata including ID, description, schedule, and owner
- Creation and updating of Dataplex Catalog entries for each DAG
- Error handling and logging for better troubleshooting

## Customization

You can modify the DAG to fit your specific needs:

- Adjust the schedule interval in the DAG definition
- Modify the metadata extraction logic in the `create_entry` function
- Customize the Dataplex entry creation in the `create_entry` function

## Troubleshooting

If you encounter issues:

1. Check Airflow logs for detailed error messages
2. Ensure all required permissions are set correctly and that Composer/Airflow service account has access to Dataplex API Catalog metadata
3. Verify that the Dataplex API is enabled and accessible

## Contributing

Contributions to improve this demonstration DAG are welcome. Please submit a pull request with your proposed changes.

## License

This project is licensed under the Apache License, Version 2.0. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
