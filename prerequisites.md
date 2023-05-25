<img src="images/prereq/phData_banner.png" width=1200px>

## SNOWFLAKE PREREQUISITES
**You'll need a Snowflake account with a user created with ACCOUNTADMIN permissions.**
This user will be used to get things set up in Snowflake.
- It is strongly recommended to sign up for a free 30 day trial Snowflake account for this lab. Once you’ve
registered, you’ll get an email that will bring you to Snowflake so that you can sign in.
- **Make sure to Activate your account** and pick a username and password that you will remember. This will
be important for logging in later on.
- **Anaconda Terms & Conditions accepted. See Getting Started section in [Third-Party Packages](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages#getting-started).**

## GITHUB PREREQUISITES

### Fork and Clone Repository for Quickstart
You’ll need to create a fork of the repository for this lab in your GitHub account, which if you are reading this file you've likely already done that. However, you can check for updates to the repository and lab by visiting phData’s
[Data Engineering Pipelines with Snowpark Python](https://github.com/phdata/sfguide-data-engineering-with-snowpark-python/) associated GitHub Repository and click on the Fork
button near the top right. Complete any required fields and click Create Fork.

<img src="images/prereq/fork.png" width=800px>

By default GitHub Actions disables any workflows (or CI/CD pipelines) defined in the forked repository.
This repository contains a workflow to deploy your Snowpark Python UDF and stored procedures, which
we’ll use later on. So for now enable this workflow by opening your forked repository in GitHub, clicking on
the Actions tab near the top middle of the page, and then clicking on the I understand my workflows, go
ahead and enable them green button.

### GitHub Actions

In order for your GitHub Actions workflow to be able to connect to your Snowflake account you will need to store your Snowflake credentials in GitHub. Action Secrets in GitHub are used to securely store values/variables which will be used in your CI/CD pipelines. In this step, we will create secrets for each of the parameters.

- From the repository, click on the Settings tab near the top of the page. From the Settings page, click on the "Secrets and variables" then "Actions" tab in the left-hand navigation. The Actions secrets should be selected. For each secret listed below click on "New repository secret" near the top right and enter the name given below along with the appropriate value (adjusting as appropriate).

    Secret Name | Secret Value
    ------------|--------------
    SNOWSQL_ACCOUNT | \<myaccount\>
    SNOWSQL_USER | \<myusername\>
    SNOWSQL_PWD | \<mypassword\>
    SNOWSQL_ROLE | HOL_ROLE
    SNOWSQL_WAREHOUSE | HOL_WH
    SNOWSQL_DATABASE | HOL_DB

- Notes:
    - To get the SNOWSQL_ACCOUNT, in the Snowflake console click on your account name in the lower left, hover over your account, then select Copy account URL.

    <img src="images/prereq/get_account.png" width=600px>

    - The account is **identifier.region.cloudprovider** prior to **.snowflakecomputing.com** 

    <img src="images/prereq/account_url.png" width=600px>

### Create a GitHub Codespace

Note: This development can be done on your desktop with VS Code, however Codespaces greatly simplifies the prerequisites and complexities of local development.

<img src="images/prereq/create_codespace.png" width=600px>

- If you’ve already created a Codespace, it can be launched and stopped from this window as well.

    <img src="images/prereq/launch_codespace.png" width=600px>

- Once the Codespace is launched, you will need to install python and Snowflake extensions

    <img src="images/prereq/extensions.png" width=400px>

- Python extension installed. Search for and install the “Python” extension (from Microsoft) in the
Extensions pane in the Codespace.
- Snowflake extension installed. Search for and install the “Snowflake” extension (from Snowflake) in the
Extensions pane in the Codespace.
   - Select the Snowflake icon in the left pane of the Codespace to sign into snowflake extension using
your snowflake URL then enter your username and password.
        -  Note: to get the snowflake URL, just as you did for the GitHub secret step; in the Snowflake console click
on your account name in the lower left, hover over your account, then select Copy account URL.
- Once you are signed into the Snowflake extension, open a new terminal

    <img src="images/prereq/terminal.png" width=400px>

### Create Snowflake Credentials File
In the Codespace terminal execute the following commands:
```
mkdir ~/.snowsql
touch ~/.snowsql/config
```

Now that we've created the file, we can open it in the codespace by navigating to it:
![image](https://user-images.githubusercontent.com/7671134/234953451-8b78db0b-d02e-44df-b00c-fb9a12754167.png)

In the dialog that opens, type in the path to your config file:
```
/home/codespace/.snowsql/config
```
<img width="905" alt="image" src="https://user-images.githubusercontent.com/7671134/234953793-c7c30a0b-591b-4d99-b923-36b782ca28ff.png">


Add your account details to the config file for snowsql, which are the exact same values used for the Github secrets, be sure to save the file.

Note: we aren’t actually installing or using snowsql, just creating the credentials in the location that the snowpark_utils python file expects them to be, since we are just deploying code to Snowflake and not staging local data.

#### Create Snowsql Credentials File
```
[connections.dev]
accountname = <myaccount>
username = <myusername>
password = <mypassword>
rolename = HOL_ROLE
warehousename = HOL_WH
dbname = HOL_DB
```
### Create Anaconda Environment and Test Connection
This lab will take place inside an Anaconda virtual environment running in the Codespace. You will create and activate an Anaconda environment for this lab using the supplied conda_env.yml file. Run these commands from a terminal in the root of your local forked repository.
```
conda env create -f conda_env.yml
conda init bash
```
You will need to close and reopen the terminal, then execute:
```
conda activate pysnowpark
```
Once activated you should see `(pysnowpark)` in front of the host name
 
 <img src="images/prereq/activate_pysnowpark.png" width=800px>


Lastly, lets test that the connection is successful. To do this we'll run `test_connection.py`

```
python test_connection.py
```

If the connection test returns successful, you have completed all the prerequisites for the lab. If it returns an error message, reopen the credentials file that you created [in a previous step](#create-snowflake-credentials-file) and check the account is correctly formatted and the username and password are correct.

If you have successfully completed all the steps, congratulations you are ready for the Hands on Lab! If you completed these prerequisites prior to attending the Hands on Lab, you can stop the Codespace in Github where you launched it from, or it will automatically stop after 30 mintues