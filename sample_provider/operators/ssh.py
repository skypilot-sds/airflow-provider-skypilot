from __future__ import annotations

import warnings
from base64 import b64encode
from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from deprecated.classic import deprecated

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.utils.types import NOTSET, ArgNotSet

from sample_provider.hooks.ssh import SkySSHHook

if TYPE_CHECKING:
    from paramiko.client import SSHClient


class SkySSHOperator(BaseOperator):
    """
    SSHOperator to execute commands on given remote host using the ssh_hook.

    :param ssh_hook: predefined ssh_hook to use for remote execution.
        Either `ssh_hook` or `ssh_conn_id` needs to be provided.
    :param ssh_conn_id: :ref:`ssh connection id<howto/connection:ssh>`
        from airflow Connections. `ssh_conn_id` will be ignored if
        `ssh_hook` is provided.
    :param remote_host: remote host to connect (templated)
        Nullable. If provided, it will replace the `remote_host` which was
        defined in `ssh_hook` or predefined in the connection of `ssh_conn_id`.
    :param command: command to execute on remote host. (templated)
    :param conn_timeout: timeout (in seconds) for maintaining the connection. The default is 10 seconds.
        Nullable. If provided, it will replace the `conn_timeout` which was
        predefined in the connection of `ssh_conn_id`.
    :param cmd_timeout: timeout (in seconds) for executing the command. The default is 10 seconds.
        Nullable, `None` means no timeout. If provided, it will replace the `cmd_timeout`
        which was predefined in the connection of `ssh_conn_id`.
    :param environment: a dict of shell environment variables. Note that the
        server will reject them silently if `AcceptEnv` is not set in SSH config. (templated)
    :param get_pty: request a pseudo-terminal from the server. Set to ``True``
        to have the remote process killed upon task timeout.
        The default is ``False`` but note that `get_pty` is forced to ``True``
        when the `command` starts with ``sudo``.
    :param banner_timeout: timeout to wait for banner from the server in seconds

    If *do_xcom_push* is *True*, the numeric exit code emitted by
    the ssh session is pushed to XCom under key ``ssh_exit``.
    """

    template_fields: Sequence[str] = ("command", "environment", "cluster_name")
    ui_color = "#f4a460"

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
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
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

    def execute(self, context=None) -> bytes | str:
        result: bytes | str
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