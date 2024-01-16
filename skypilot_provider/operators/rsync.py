from __future__ import annotations

import os

from typing import Any, TYPE_CHECKING
import paramiko
from airflow import AirflowException

from sky import ClusterStatus
from sky.utils.command_runner import SSHCommandRunner
from skypilot_provider.operators.core_function import SkyBaseOperator, check_available_cluster

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SkyRsyncOperator(SkyBaseOperator):
    """Interface for Rsync operators transfer files between the airflow worker and Sky cloud instance.

    Rsync Up/Down operators are separately implemented for convenience.
    """
    template_fields = ("cluster_name", "source", "target")

    def __init__(
            self,
            *,
            cluster_name: str,
            source: str,
            target: str,
            up: bool,
            sky_home_dir: str = '/opt/airflow/sky_home_dir',
            **kwargs
    ) -> None:
        super().__init__(sky_home_dir=sky_home_dir, **kwargs)
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
            sky_home_dir: str = '/opt/airflow/sky_home_dir',
            **kwargs
    ) -> None:
        """Syncs up from the airflow worker to the sky cloud instance

        Args:
            cluster_name: The name of the target cluster previously launched by SkyLaunchOperator. Can be specified by
                str or by XComArg like "cluster_name=sky_launch_op.output".
            source: The path of the source file or directory in the airflow worker.
            target" The target path in the sky cloud instance.
        """

        super().__init__(
            cluster_name=cluster_name,
            source=source,
            target=target,
            up=True,
            sky_home_dir=sky_home_dir,
            **kwargs)

    def _sky_execute(self, context: Context) -> Any:
        return self._rsync(context)


class SkyRsyncDownOperator(SkyRsyncOperator):
    def __init__(
            self,
            *,
            cluster_name: str,
            source: str,
            target: str,
            sky_home_dir: str = '/opt/airflow/sky_home_dir',
            **kwargs
    ) -> None:
        """Syncs down from the sky cloud instance to airflow worker.

        Args:
            cluster_name: The name of the target cluster previously launched by SkyLaunchOperator. Can be specified by
                str or by XComArg like "cluster_name=sky_launch_op.output".
            source: The path of the source file or directory in the sky cloud instance.
            target" The target path in the airflow worker.
        """
        super().__init__(
            cluster_name=cluster_name,
            source=source,
            target=target,
            up=False,
            sky_home_dir = sky_home_dir,
            **kwargs)

    def _sky_execute(self, context: Context) -> Any:
        return self._rsync(context)


