from __future__ import annotations

from base64 import b64encode
from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.utils.types import NOTSET, ArgNotSet
from sky import ClusterStatus

from skypilot_provider.hooks.sky_ssh import SkySSHHook
from skypilot_provider.operators.core_function import check_available_cluster, SkyBaseOperator

if TYPE_CHECKING:
    from paramiko.client import SSHClient


class SkySSHOperator(SkyBaseOperator):

    template_fields: Sequence[str] = ("command", "environment", "cluster_name")

    def __init__(
        self,
        *,
        sky_ssh_hook: SkySSHHook | None = None,
        cluster_name: str | None = None,
        command: str | None = None,
        conn_timeout: int | None = None,
        cmd_timeout: int | ArgNotSet | None = NOTSET,
        environment: dict | None = None,
        get_pty: bool = False,
        sky_home_dir: str = '/opt/airflow/sky_home_dir',
        **kwargs,
    ) -> None:
        """SkySSHOperator to execute commands on given Sky cloud instance.
        Args:
            ssh_hook: predefined sky_ssh_hook to use to connect sky cloud instance.
                Either `sky_ssh_hook` or `cluster_name` needs to be provided.
            cluster_name: The name of the target cluster previously launched by SkyLaunchOperator. Can be specified by
                str or by XComArg like "cluster_name=sky_launch_op.output". `cluster_name` will be ignored if
                `sky_ssh_hook` is provided.
            command: Command to execute on remote host. (templated)
            conn_timeout: timeout (in seconds) for maintaining the connection. The default is 10 seconds.
            cmd_timeout: timeout (in seconds) for executing the command. The default is 10 seconds.
            environment: a dict of shell environment variables. Note that the
                server will reject them silently if `AcceptEnv` is not set in SSH config. (templated)
            get_pty: request a pseudo-terminal from the server. Set to ``True``
                to have the remote process killed upon task timeout.
                The default is ``False`` but note that `get_pty` is forced to ``True``
                when the `command` starts with ``sudo``.
        """
        super().__init__(sky_home_dir=sky_home_dir, **kwargs)
        if sky_ssh_hook and isinstance(sky_ssh_hook, SkySSHHook):
            self.ssh_hook = sky_ssh_hook

        self.cluster_name = cluster_name
        self.command = command
        self.conn_timeout = conn_timeout
        self.cmd_timeout = cmd_timeout
        self.environment = environment
        self.get_pty = get_pty

    @cached_property
    def ssh_hook(self) -> SkySSHHook:
        """Create SSHHook to run commands on remote host."""
        if self.cluster_name:
            self.log.info("sky_ssh_hook is not provided or invalid. Trying cluster_name to create SSHHook.")
            hook = SkySSHHook(cluster_name=self.cluster_name)
            return hook
        raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

    @property
    def hook(self) -> SkySSHHook:
        return self.ssh_hook

    def get_ssh_client(self) -> SSHClient:
        # Remember to use context manager or call .close() on this when done
        self.log.info("Creating ssh_client")
        return self.hook.get_conn()

    def raise_for_status(self, exit_status: int, stderr: bytes, context=None) -> None:
        if context and self.do_xcom_push:
            ti = context.get("task_instance")
            ti.xcom_push(key="ssh_exit", value=exit_status)
        if exit_status != 0:
            raise AirflowException(f"SSH operator error: exit status = {exit_status}")

    def run_ssh_client_command(self, ssh_client: SSHClient, command: str, context=None) -> bytes:
        exit_status, agg_stdout, agg_stderr = self.hook.exec_ssh_client_command(
            ssh_client, command, timeout=self.cmd_timeout, environment=self.environment, get_pty=self.get_pty
        )
        self.raise_for_status(exit_status, agg_stderr, context=context)
        return agg_stdout

    def _sky_execute(self, context=None) -> bytes | str:
        result: bytes | str

        check_available_cluster(self.cluster_name, [ClusterStatus.UP])

        if self.command is None:
            raise AirflowException("SSH operator error: SSH command not specified. Aborting.")

        # Forcing get_pty to True if the command begins with "sudo".
        self.get_pty = self.command.startswith("sudo") or self.get_pty

        with self.get_ssh_client() as ssh_client:
            result = self.run_ssh_client_command(ssh_client, self.command, context=context)
        enable_pickling = conf.getboolean("core", "enable_xcom_pickling")
        if not enable_pickling:
            result = b64encode(result).decode("utf-8")
        return result
