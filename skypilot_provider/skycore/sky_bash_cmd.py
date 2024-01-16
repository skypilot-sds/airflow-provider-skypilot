from __future__ import annotations
import os
import re
import shutil
from functools import cached_property


from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.utils.context import  Context


import contextlib
import os
import signal
from collections import namedtuple
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory, gettempdir

from airflow.hooks.base import BaseHook

SubprocessResult = namedtuple("SubprocessResult", ["exit_code", "output"])


class SubprocessHook(BaseHook):
    """Hook for running processes with the ``subprocess`` module."""

    def __init__(self) -> None:
        self.sub_process: Popen[bytes] | None = None
        super().__init__()

    def run_command(
        self,
        command: list[str],
        env: dict[str, str] | None = None,
        output_encoding: str = "utf-8",
        cwd: str | None = None,
        line_capture_query: list[str] | None = None
    ) -> SubprocessResult:
        """
        Execute the command.

        If ``cwd`` is None, execute the command in a temporary directory which will be cleaned afterwards.
        If ``env`` is not supplied, ``os.environ`` is passed

        :param command: the command to run
        :param env: Optional dict containing environment variables to be made available to the shell
            environment in which ``command`` will be executed.  If omitted, ``os.environ`` will be used.
            Note, that in case you have Sentry configured, original variables from the environment
            will also be passed to the subprocess with ``SUBPROCESS_`` prefix. See
            :doc:`/administration-and-deployment/logging-monitoring/errors` for details.
        :param output_encoding: encoding to use for decoding stdout
        :param cwd: Working directory to run the command in.
            If None (default), the command is run in a temporary directory.
        :return: :class:`namedtuple` containing ``exit_code`` and ``output``, the last line from stderr
            or stdout
        """
        self.log.info("Tmp dir root location: %s", gettempdir())

        if line_capture_query: line_captured = {key: [] for key in line_capture_query}
        with contextlib.ExitStack() as stack:
            if cwd is None:
                cwd = stack.enter_context(TemporaryDirectory(prefix="airflowtmp"))

            def pre_exec():
                # Restore default signal disposition and invoke setsid
                for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            self.log.info("Running command: %s", command)

            self.sub_process = Popen(
                command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=cwd,
                env=env if env or env == {} else os.environ,
                preexec_fn=pre_exec,
            )

            # ansi_escape_8bit = re.compile(br'(?:\x1B[@-Z\\-_]|[\x80-\x9A\x9C-\x9F]|(?:\x1B\[|\x9B)[0-?]*[ -/]*[@-~])')
            ansi_escape_8bit = re.compile(br'(?:\x1B[@-Z\\-_]|(?:\x1B\[|\x9B)[0-?]*[ -/]*[@-~])')


            self.log.info("Output:")
            line = ""

            if self.sub_process is None:
                raise RuntimeError("The subprocess should be created here and is None!")
            if self.sub_process.stdout is not None:
                for raw_line in iter(self.sub_process.stdout.readline, b""):
                    raw_line = ansi_escape_8bit.sub(b'', raw_line)
                    line = raw_line.decode(output_encoding, errors="backslashreplace").rstrip()
                    sublines = line.split('\r')
                    for subline in sublines:
                        self.log.info("%s", subline)
                        if line_capture_query:
                            for line_key in line_capture_query:
                                if line_key in subline: line_captured[line_key].append(subline)

            self.sub_process.wait()

            self.log.info("Command exited with return code %s", self.sub_process.returncode)
            return_code: int = self.sub_process.returncode

        output = line_captured if line_capture_query else None
        return SubprocessResult(exit_code=return_code, output=output)

    def send_sigterm(self):
        """Send SIGTERM signal to ``self.sub_process`` if one exists."""
        self.log.info("Sending SIGTERM signal to process group")
        if self.sub_process and hasattr(self.sub_process, "pid"):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)


class SkyBashCmd:
    def __init__(self,
                 bash_command: str,
                 output_encoding: str = 'utf-8'):
        self.bash_command = bash_command
        self.output_encoding = output_encoding

    @cached_property
    def subprocess_hook(self):
        return SubprocessHook()


    def get_env(self, context):
        system_env = os.environ.copy()
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        system_env.update(airflow_context_vars)

    def bash_execute(self, context:Context):
        bash_path = shutil.which('bash') or 'bash'
        env = self.get_env(context)
        result = self.subprocess_hook.run_command(
            command=[bash_path, '-c', self.bash_command],
            env = env,
            output_encoding=self.output_encoding,
            cwd = os.getcwd()
        )
        return result.output

    #Running task on cluster
    def bash_exec_and_get_matched_line(self,context:Context, query):
        bash_path = shutil.which('bash') or 'bash'
        env = self.get_env(context)
        result = self.subprocess_hook.run_command(
            command=[bash_path, '-c', self.bash_command],
            env = env,
            output_encoding=self.output_encoding,
            cwd = os.getcwd(),
            line_capture_query = query
        )
        return result.output