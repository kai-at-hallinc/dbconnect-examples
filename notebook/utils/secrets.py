import configparser
import os


def retrieve(secret):
    try:
        config = configparser.ConfigParser()
        config.read("../databricks_connect.ini")

        # Check if the section 'Databricks' exists
        if "Databricks" not in config:
            raise KeyError("Section 'Databricks' not found in the configuration file.")

        # Resolve environment variables
        for key in config["Databricks"]:
            config["Databricks"][key] = os.path.expandvars(config["Databricks"][key])

        # Return needed secret
        return config["Databricks"][secret]

    except KeyError as e:
        print(e)
        # Handle the error or return a default value
        return None
