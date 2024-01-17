__version__ = "0.1.3"

## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "airflow-provider-skypilot",  # Required
        "name": "Skypilot",  # Required
        "description": "Skypilot provider for Apache Airflow providers.",  # Required
        "connection-types": [
            {
                "connection-type": "sky_ssh",
                "hook-class-name": "skypilot_provider.hooks.sky_ssh.SkySSHHook"
            }
        ],
        "versions": [__version__],  # Required
    }
