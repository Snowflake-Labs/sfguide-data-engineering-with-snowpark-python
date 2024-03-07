import sys
import os
import yaml

ignore_folders = ['.git', '__pycache__', '.ipynb_checkpoints']
snowflake_project_config_filename = 'snowflake.yml'

if len(sys.argv) != 2:
    print("Root directory is required")
    exit()

root_directory = sys.argv[1]
print(f"Deploying all Snowpark apps in root directory {root_directory}")

# Walk the entire directory structure recursively
for (directory_path, directory_names, file_names) in os.walk(root_directory):
    # Get just the last/final folder name in the directory path
    base_name = os.path.basename(directory_path)

    # Skip any folders we want to ignore
    # TODO: Update this logic to skip all subfolders of ignored folder
    if base_name in ignore_folders:
#        print(f"Skipping ignored folder {directory_path}")
        continue

    # An snowflake.yml file in the folder is our indication that this folder contains
    # a Snow CLI project
    if not snowflake_project_config_filename in file_names:
#        print(f"Skipping non-app folder {directory_path}")
        continue
    print(f"Found Snowflake project in folder {directory_path}")

    # Read the project config
    project_settings = {}
    with open(f"{directory_path}/{snowflake_project_config_filename}", "r") as yamlfile:
        project_settings = yaml.load(yamlfile, Loader=yaml.FullLoader)

    # Confirm that this is a Snowpark project
    # TODO: Would be better if the project config file had a project_type key!
    if 'snowpark' not in project_settings:
        print(f"Skipping non Snowpark project in folder {base_name}")
        continue

    # Finally deploy the Snowpark project with the snowcli tool
    print(f"Found Snowflake Snowpark project '{project_settings['snowpark']['project_name']}' in folder {base_name}")
    print(f"Calling snowcli to deploy the project")
    os.chdir(f"{directory_path}")
    # Make sure all 6 SNOWFLAKE_ environment variables are set
    # SnowCLI accesses the passowrd directly from the SNOWFLAKE_PASSWORD environmnet variable
    os.system(f"snow snowpark build")
    os.system(f"snow snowpark deploy --replace --temporary-connection --account $SNOWFLAKE_ACCOUNT --user $SNOWFLAKE_USER --role $SNOWFLAKE_ROLE --warehouse $SNOWFLAKE_WAREHOUSE --database $SNOWFLAKE_DATABASE")
