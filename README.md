<p align="center">
  <a href="https://www.airflow.apache.org">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" /> 
  </a>
  <a href="https://skypilot.readthedocs.io">
    <img alt="Airflow" src="https://raw.githubusercontent.com/skypilot-org/skypilot/master/docs/source/images/skypilot-wide-light-1k.png" height="60" />
  </a>
</p>
<h1 align="center">
  Apache Airflow Provider for SkyPilot
</h1>
  <h3 align="center">
A provider you can utilize multiple clouds on Apache Airflow through SkyPilot.
</h3>

<br/>

## Installation

The SkyPilot provider for Apache Airflow was developed and tested on an environment with the following dependencies installed:

- [Apache Airflow](https://airflow.apache.org/) >= 2.3
- [SkyPilot](https://skypilot.readthedocs.io) >= 0.4

The installation of the SkyPilot provider may start from the Airflow environment configured with Docker instructed in ["Running Airflow in Docker"](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).
Base on the docker configuration, add `pip install` command in the Dockerfile and build your own Docker image. 
```Dockerfile
RUN pip install --user airflow-provider-skypilot
```

Then, make sure that SkyPilot is properly installed and initialized on the same environment. The initialization includes cloud account setup and access verification. 
Please refer to [SkyPilot Installation](https://skypilot.readthedocs.io/en/latest/getting-started/installation.html) 
for more information 



## Configuration 
The SkyPilot provider runs on a Airflow worker, but it shares and stores its metadata in the Airflow master node. 
This scheme allows the execution of consecutive sky tasks run across independent workers by sharing the metadata.

Following settings in the `docker-compose.yaml` configure the data sharing, including cloud credentials, metadata and workspace. 

```yaml
x-airflow-common:
  environment:
    volumes:
      - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
      - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
      - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
      - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
      # mount cloud credentials
      - ${HOME}/.aws:/opt/airflow/sky_home_dir/.aws
      - ${HOME}/.azure:/opt/airflow/sky_home_dir/.azure
      - ${HOME}/.config/gcloud:/opt/airflow/sky_home_dir/.config/gcloud
      - ${HOME}/.scp:/opt/airflow/sky_home_dir/.scp
      # mount sky metadata 
      - ${HOME}/.sky:/opt/airflow/sky_home_dir/.sky
      - ${HOME}/.ssh:/opt/airflow/sky_home_dir/.ssh
      # mount sky working dir
      - ${HOME}/sky_workdir:/opt/airflow/sky_home_dir/sky_workdir
```
This example mounts cloud credentials for `AWS`, `Azure`, `GCP`, and `SCP`. 
For SkyPilot metadata, mount `.sky/` and `.ssh/` directories. 
The directory `sky_workdir/` is to share user resources including user codes and `yaml` task definition for Skypilot execution.
> Note that all sky directories are mounted under `sky_home_dir/`. 
> They will be symbolic-linked to `${HOME}/` in workers by the setting an arguments `sky_home_dir='/opt/airflow/sky_home_dir'` of Sky operators.  




















