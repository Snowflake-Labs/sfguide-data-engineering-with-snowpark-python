import sys;
import os;

ignore_folders = ['__pycache__', '.ipynb_checkpoints']

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
    if base_name in ignore_folders:
#        print(f"Skipping ignored folder {directory_path}")
        continue

    # An app.toml file in the folder is our indication that this folder contains
    # a snowcli Snowpark App
    if not "app.toml" in file_names:
#        print(f"Skipping non-app folder {directory_path}")
        continue

    # Next determine what type of app it is
    app_type = "unknown"
    if "local_connection.py" in file_names:
        app_type = "procedure"
    else:
        app_type = "function"

    # Finally deploy the app with the snowcli tool
    print(f"Found {app_type} app in folder {directory_path}")
    print(f"Calling snowcli to deploy the {app_type} app")
    os.chdir(f"{directory_path}")
    # snow login will update the app.toml file with the correct path to the snowsql config file
    os.system(f"snow login -c {root_directory}/config -C dev")
    os.system(f"snow {app_type} create")
