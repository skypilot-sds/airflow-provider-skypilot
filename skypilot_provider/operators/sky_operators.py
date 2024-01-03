from __future__ import annotations

import logging
import os

from typing import Any, TYPE_CHECKING

import paramiko
from airflow import AirflowException
from airflow.models import BaseOperator

import sky
from sky.backends import backend_utils
from sky.cli import _make_task_or_dag_from_entrypoint_with_overrides
from sky import global_user_state, ClusterStatus, backends
from sky.utils.command_runner import SSHCommandRunner

from skypilot_provider.skycore.sky_bash_cmd import SkyBashCmd
from skypilot_provider.skycore.sky_core import CloudVmRayBackendAirExtend
from skypilot_provider.skycore.sky_log import SkyLogFilter

if TYPE_CHECKING:
    from airflow.utils.context import Context


def check_available_cluster(cluster_name, available_status_list):
    cluster_records = sky.status(cluster_names=[cluster_name], refresh=True)
    if len(cluster_records) == 0:
        raise AirflowException(f'Cluster {cluster_name} does not exist in SkyPilot DB.')
    cluster_record = cluster_records[0]
    status = cluster_record['status']
    if status not in available_status_list:
        raise AirflowException(f'Status of {cluster_name} should be {str(available_status_list)}, but it is {status}')


class SkyOperator(BaseOperator):
    """Interface for Sky operators.

    This interface adds a function to arrange the logs from Sky operators.
    Subclasses are  responsible to implement _sky_execute function for their own functionality
    """

    ui_color = "#82A8DC"

    def __init__(
            self,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.user_home_dir = None
        self.pre_cwd = None

    def execute(self, context: Context) -> Any:

        self.user_home_dir = os.path.expanduser('~')
        self.pre_cwd = os.getcwd()
        os.chdir(self.user_home_dir)
        self._add_sky_format_logger()

        result = self._sky_execute(context)

        self._remove_sky_format_log_handler()
        os.chdir(self.pre_cwd)
        return result

    def _sky_execute(self, context: Context) -> Any:
        raise NotImplementedError("SkyOperator must be accessed from its subclasses")

    def _add_sky_format_logger(self):
        """Adds log filter for the formatting of logs from SkyPilot"""
        sky_filter: SkyLogFilter | None = None

        for f in self.log.filters:
            if isinstance(f, SkyLogFilter):
                sky_filter = f
                break

        if sky_filter is None:
            self.log.addFilter(SkyLogFilter())

        from sky import optimizer
        from sky.provision import provisioner
        from sky.backends import cloud_vm_ray_backend

        optimizer.logger = self.log
        provisioner.logger = self.log
        cloud_vm_ray_backend.logger = self.log

    def _remove_sky_format_log_handler(self):
        """Removes the added log filter at the end of the main function"""
        sky_filters = []
        for h in self.log.filters:
            if isinstance(h, SkyLogFilter):
                sky_filters.append(h)

        for h in sky_filters:
            self.log.removeFilter(h)


class SkyLaunchOperator(SkyOperator):
    def __init__(
            self,
            *,
            sky_task_yaml: str,
            cloud: str = "cheapest",
            gpus: str | None = None,
            minimum_cpus: int | None = None,
            minimum_memory: int | None = None,
            disk_size: int | None = None,
            auto_down: bool = True,
            image_id: str | None = None,
            sky_home_dir: str | None = '/opt/airflow/sky_home_dir',
            **kwargs
    ) -> None:
        """Launches a cluster and executes a sky task.

        This operator mimics Launch command of Skypilot.
        Please refer to the Skypilot official site (https://skypilot.readthedocs.io) for more detailed functionality.

        Args:
            sky_task_yaml: The path of YAML file defining Skypilot tasks.
            cloud: name of SCP, should be one of CSPs supported by Skypilot. If 'cheapest" or None,
                the cheapest cloud provider would be selected by Skypilot optimizer.
            gpus: specifications for the GPU model and number of the cores, like "A100:4"
            minimum_cpus: minimum number of CPUS each instance should have.
            minimum_memory: minimum memory each instance should have.
            disk_size: disk size of the instance in GB.
            auto_down: If True, the cluster will be terminated right after the task is finished.
            image_id: If None, the default image specified by Skypilot will be used.
            sky_home_dir: The files and directories in this dir will be copied (symbolic-linked) into user home
                directory. This is used to mount CSP credential files and user's codes from airflow host machine
                to the worker where the sky operator actually runs. If None, the copy process will be skipped .
        Note:
            Cloud settings specified by the args override the settings in the YAML, just like Skypilot does.
        """

        super().__init__(**kwargs)

        self.sky_task_yaml = os.path.expanduser(sky_task_yaml) if '~' in sky_task_yaml else sky_task_yaml
        self.cloud_provider = None if cloud == "cheapest" else cloud
        self.gpus = gpus
        self.minimum_cpus = None if minimum_cpus is None else str(minimum_cpus) + '+'
        self.minimum_memory = None if minimum_memory is None else str(minimum_memory) + '+'
        self.disk_size = disk_size
        self.auto_down = auto_down
        self.image_id = image_id
        self.sky_home_dir = sky_home_dir

    def _cp_env_dir(self) -> None:
        for item in os.listdir(self.sky_home_dir):
            src = os.path.join(self.sky_home_dir, item)
            dest = os.path.join(self.user_home_dir, item)
            if os.path.exists(dest): continue
            os.symlink(src, dest)

    def _sky_execute(self, context: Context) -> Any:
        """
        Mimics Sky Launch command. Sky check is executed before the launch.

        Returns:
            cluster: The name of the launched cluster for further use.
        """

        if self.cloud_provider in ['local', 'k8s']:
            raise NotImplementedError(f'{self.cloud_provider} is not supported yet')

        if self.sky_home_dir:
            self._cp_env_dir()

        check_cmd = SkyBashCmd(bash_command="sky check")
        check_cmd.bash_execute(context)

        enabled_clouds = global_user_state.get_enabled_clouds()
        if len(enabled_clouds) == 0:
            raise AirflowException(f"No CSP is enabled. Please copy CSP credential files into {self.sky_home_dir}")

        cluster = self._launch()

        if self.auto_down:
            sky.down(cluster)
            self.log.info(f'Cluster {cluster} Terminated.')

        self.log.info("Done")
        return cluster

    def _launch(self):
        task = _make_task_or_dag_from_entrypoint_with_overrides(
            entrypoint=[self.sky_task_yaml],
            cloud=self.cloud_provider,
            gpus=self.gpus,
            cpus=self.minimum_cpus,
            memory=self.minimum_memory,
            disk_size=self.disk_size,
            image_id=self.image_id
        )
        cluster_name = backend_utils.generate_cluster_name()
        backend = CloudVmRayBackendAirExtend(self.log)

        sky.launch(
            task,
            backend=backend,
            cluster_name=cluster_name,
            stream_logs=True,
        )
        return cluster_name


class SkyExecOperator(SkyOperator):
    template_fields = ("cluster_name",)

    def __init__(
            self,
            *,
            cluster_name: str,
            sky_task_yaml: str,
            sky_home_dir: str | None = '/opt/airflow/sky_home_dir',
            **kwargs
    ) -> None:
        """Executes a sky task on the specified cluster

        This operator mimics Exec command of Skypilot.
        Please refer to the Skypilot official site (https://skypilot.readthedocs.io) for more detailed functionality.

        Args:
            cluster_name: The name of the target cluster previously launched by SkyLaunchOperator. Can be specified by
                str or by XComArg like "cluster_name=sky_launch_op.output".
            sky_task_yaml: The path of YAML file defining Skypilot tasks.
            sky_home_dir: The files and directories in this dir will be copied (symbolic-linked) into user home
                directory. This is used to mount CSP credential files and user's codes from airflow host machine
                to the worker where the sky operator actually runs. If None, the copy process will be skipped .
       """

        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.sky_task_yaml = os.path.expanduser(sky_task_yaml) if '~' in sky_task_yaml else sky_task_yaml
        self.sky_home_dir = sky_home_dir

    def _cp_env_dir(self) -> None:
        for item in os.listdir(self.sky_home_dir):
            src = os.path.join(self.sky_home_dir, item)
            dest = os.path.join(self.user_home_dir, item)
            if os.path.exists(dest): continue
            os.symlink(src, dest)

    def _sky_execute(self, context: Context) -> Any:
        """
        Mimics Sky Exec command.

        Returns:
            cluster: The name of the cluster for further use.
        """
        check_available_cluster(self.cluster_name, [ClusterStatus.UP])
        if self.sky_home_dir:
            self._cp_env_dir()

        cluster = self._exec(context)
        self.log.info("Done")
        return cluster

    def _exec(self, context):
        handle = global_user_state.get_handle_from_cluster_name(self.cluster_name)
        if not isinstance(handle, backends.CloudVmRayResourceHandle):
            raise AirflowException(f'{self.cluster_name} is not an Airflow cluster')
        backend = CloudVmRayBackendAirExtend(self.log)

        task = _make_task_or_dag_from_entrypoint_with_overrides(
            entrypoint=[self.sky_task_yaml],
            cluster= self.cluster_name
        )
        sky.exec(task, backend=backend, cluster_name=self.cluster_name)

        return self.cluster_name


class SkyRsyncOperator(SkyOperator):
    """Interface for Rsync operators transfer files between the airflow worker and Sky cloud instance.

    Rsync Up/Down operators are separately implemented for convenience.
    """
    template_fields = ("cluster_name",)

    def __init__(
            self,
            *,
            cluster_name: str,
            source: str,
            target: str,
            up: bool,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.source = source
        self.target = target
        self.up = up

    def _sky_execute(self, context: Context) -> Any:
        raise NotImplementedError("SkyRsyncOperator must be accessed from its subclasses")

    def _rsync(self, context: Context) -> Any:
        check_available_cluster(self.cluster_name, [ClusterStatus.UP])
        conf_file = os.path.expanduser("~/.ssh/config")
        if not os.path.exists(conf_file):
            raise AirflowException(f"{conf_file} does not exist")

        config = paramiko.SSHConfig()

        config.parse(open(conf_file))
        info = config.lookup(self.cluster_name)
        ident = info['identityfile']
        if type(ident) is list:
            ident = ident[0]

        sky_ssh_comm_runner = SSHCommandRunner(ip=info['hostname'],
                                               ssh_user=info['user'],
                                               ssh_private_key=ident,
                                               port=int(info['port']))

        sky_ssh_comm_runner.rsync(source=self.source,
                                  target=self.target,
                                  up=self.up,
                                  stream_logs=True)
        return self.cluster_name


class SkyRsyncUpOperator(SkyRsyncOperator):
    def __init__(
            self,
            *,
            cluster_name: str,
            source: str,
            target: str,
            **kwargs
    ) -> None:
        """Syncs up from the airflow worker to the sky cloud instance

        Args:
            cluster_name: The name of the target cluster previously launched by SkyLaunchOperator. Can be specified by
                str or by XComArg like "cluster_name=sky_launch_op.output".
            source: The path of the source file or directory in the airflow worker.
            target" The target path in the sky cloud instance.
        """

        super().__init__(cluster_name=cluster_name, source=source, target=target, up=True, **kwargs)

    def _sky_execute(self, context: Context) -> Any:
        return self._rsync(context)


class SkyRsyncDownOperator(SkyRsyncOperator):
    def __init__(
            self,
            *,
            cluster_name: str,
            source: str,
            target: str,
            **kwargs
    ) -> None:
        """Syncs down from the sky cloud instance to airflow worker.

        Args:
            cluster_name: The name of the target cluster previously launched by SkyLaunchOperator. Can be specified by
                str or by XComArg like "cluster_name=sky_launch_op.output".
            source: The path of the source file or directory in the sky cloud instance.
            target" The target path in the airflow worker.
        """
        super().__init__(cluster_name=cluster_name, source=source, target=target, up=False, **kwargs)

    def _sky_execute(self, context: Context) -> Any:
        return self._rsync(context)


class SkyDownOperator(SkyOperator):
    template_fields = ("cluster_name",)

    def __init__(
            self,
            *,
            cluster_name: str,
            **kwargs
    ) -> None:
        """Syncs down from the sky cloud instance to airflow worker.

        Args:
            cluster_name: The name of the target cluster previously launched by SkyLaunchOperator. Can be specified by
                str or by XComArg like "cluster_name=sky_launch_op.output".
        """
        super().__init__(**kwargs)
        self.cluster_name = cluster_name

    def _sky_execute(self, context: Context) -> Any:
        check_available_cluster(self.cluster_name, [ClusterStatus.UP, ClusterStatus.INIT, ClusterStatus.STOPPED])
        sky.down(self.cluster_name)
        self.log.info(f'Cluster {self.cluster_name} Terminated.')
        return self.cluster_name


