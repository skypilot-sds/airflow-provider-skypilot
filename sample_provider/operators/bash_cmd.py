import os
import shutil
from functools import cached_property

from airflow.hooks.subprocess import SubprocessHook
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.utils.context import  Context


class BashCmd:
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