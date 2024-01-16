from __future__ import annotations
import paramiko
import os

from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.types import ArgNotSet, NOTSET
from select import select

TIMEOUT_DEFAULT = 10
CMD_TIMEOUT = 10


class SkySSHHook(BaseHook):

    conn_name_attr = 'sky_ssh_conn_id'
    default_conn_name = 'sky_ssh_default'
    conn_type = 'sky_ssh'
    hook_name = 'Sky_ssh'

    def __init__(self,
                 cluster_name:str ):
        super().__init__()
        self.remote_host = cluster_name
        self.client = None

    def get_conn(self) -> paramiko.SSHClient:

        if not os.path.exists(os.path.expanduser("~/.ssh/config")):
            raise AirflowException("~/.ssh/config does not exist")

        config = paramiko.SSHConfig()
        conf_file = os.path.expanduser("~/.ssh/config")
        config.parse(open(conf_file))
        info = config.lookup(self.remote_host)
        ident = info['identityfile']
        if type(ident) is list :
            ident = ident[0]

        con_arg = {'hostname' : info['hostname'],
                       'port': info['port'],
                       'username': info['user'],
                       'key_filename': ident}

        client = paramiko.SSHClient()

        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(**con_arg)
        self.client = client
        return client


    def exec_ssh_client_command(
        self,
        ssh_client: paramiko.SSHClient,
        command: str,
        get_pty: bool,
        environment: dict | None,
        timeout: int | ArgNotSet | None = NOTSET,
    ) -> tuple[int, bytes, bytes]:
        self.log.info("Running command: %s", command)

        cmd_timeout: int | None
        if not isinstance(timeout, ArgNotSet):
            cmd_timeout = timeout
        else:
            cmd_timeout = CMD_TIMEOUT
        del timeout  # Too easy to confuse with "timedout" below.

        # set timeout taken as params
        stdin, stdout, stderr = ssh_client.exec_command(
            command=command,
            get_pty=get_pty,
            timeout=cmd_timeout,
            environment=environment,
        )
        # get channels
        channel = stdout.channel

        # closing stdin
        stdin.close()
        channel.shutdown_write()

        agg_stdout = b""
        agg_stderr = b""

        # capture any initial output in case channel is closed already
        stdout_buffer_length = len(stdout.channel.in_buffer)

        if stdout_buffer_length > 0:
            agg_stdout += stdout.channel.recv(stdout_buffer_length)

        timedout = False

        # read from both stdout and stderr
        while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready():
            readq, _, _ = select([channel], [], [], cmd_timeout)
            if cmd_timeout is not None:
                timedout = not readq
            for recv in readq:
                if recv.recv_ready():
                    output = stdout.channel.recv(len(recv.in_buffer))
                    agg_stdout += output
                    for line in output.decode("utf-8", "replace").strip("\n").splitlines():
                        self.log.info(line)
                if recv.recv_stderr_ready():
                    output = stderr.channel.recv_stderr(len(recv.in_stderr_buffer))
                    agg_stderr += output
                    for line in output.decode("utf-8", "replace").strip("\n").splitlines():
                        self.log.warning(line)
            if (
                stdout.channel.exit_status_ready()
                and not stderr.channel.recv_stderr_ready()
                and not stdout.channel.recv_ready()
            ) or timedout:
                stdout.channel.shutdown_read()
                try:
                    stdout.channel.close()
                except Exception:
                    # there is a race that when shutdown_read has been called and when
                    # you try to close the connection, the socket is already closed
                    # We should ignore such errors (but we should log them with warning)
                    self.log.warning("Ignoring exception on close", exc_info=True)
                break

        stdout.close()
        stderr.close()

        if timedout:
            raise AirflowException("SSH command timed out")

        exit_status = stdout.channel.recv_exit_status()

        return exit_status, agg_stdout, agg_stderr