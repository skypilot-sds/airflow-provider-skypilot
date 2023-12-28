from __future__ import annotations

import os
import re
import sys

os.environ['SKYPILOT_MINIMIZE_LOGGING'] = '1'
from typing import Any, TYPE_CHECKING


from airflow.models import BaseOperator
from sky import backends


from sample_provider.operators.bash_cmd import BashCmd
from sample_provider.operators.sky_core import CloudVmRayBackendAirExtend

if TYPE_CHECKING:
    from airflow.utils.context import Context





class RedirectFormmatter:
    def __init__(self, oldout):
        self.out = oldout
        self.ansi_escape = re.compile(r'(?:\x1B[@-Z\\-_]|(?:\x1B\[|\x9B)[0-?]*[ -/]*[@-~])')
    def write(self, msg):
        raw_lines = self.ansi_escape.sub('', msg)
        sub_lines = raw_lines.split('\n')
        for line in sub_lines:
            self.out.write("!!!!!!!!!!!!!!!!!"+ line)
    def flush(self):
        self.out.flush()




class SkyLaunchOperator(BaseOperator):
    template_fields = [
        "op_option_list"
    ]
    template_fields_renderers = {"op_option_list": "py"}
    ui_color = "#82A8DC"

    def __init__(
            self,
            *,
            sky_task_yaml: str,
            op_option_list: str | None = None,
            auto_down=True,
            sky_working_dir: str = '/opt/airflow/sky_workdir',
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        if '~' in sky_task_yaml: sky_task_yaml = os.path.expanduser(sky_task_yaml)
        self.sky_task_yaml = sky_task_yaml
        self.cmd_lines = [sky_task_yaml]
        self.op_option_list = op_option_list or []
        self.auto_down = auto_down

        if '-y' not in self.op_option_list and '--yes' not in self.op_option_list: self.op_option_list.append("--yes")
        self.cmd_lines.extend(self.op_option_list)
        self.sky_working_dir = sky_working_dir
        self.home_dir = os.path.expanduser('~')


    def cp_env_dir(self) -> None:

        for item in os.listdir(self.sky_working_dir):
            src = os.path.join(self.sky_working_dir, item)
            dest = os.path.join(self.home_dir, item)
            if os.path.exists(dest): continue
            os.symlink(src, dest)

    def execute(self, context: Context) -> Any:
        sys.stdout = RedirectFormmatter(sys.stdout)
        import sky
        self.pre_cwd = os.getcwd()
        os.chdir(self.home_dir)
        self.cp_env_dir()

        check_cmd = BashCmd(bash_command="sky check")
        check_cmd.bash_execute(context)
        from sky import global_user_state
        enabled_clouds = global_user_state.get_enabled_clouds()
        if len(enabled_clouds) == 0: self.end_exec()

        cluster = self.launch(context)

        if self.auto_down:
            sky.down(cluster)
            self.log.info(f'Cluster {cluster} Terminated.')

        self.log.info("Done")
        self.end_exec()
        return cluster

    def end_exec(self):
        os.chdir(self.pre_cwd)
        return

    def launch(self, context):
        import sky
        from sky.cli import _make_task_or_dag_from_entrypoint_with_overrides
        self.provider = 'cheapest'
        self.num_instances = 1
        task = _make_task_or_dag_from_entrypoint_with_overrides(entrypoint=[self.sky_task_yaml])

        # if Path("~/.rh").expanduser().exists():
        #     task.set_file_mounts(
        #         {
        #             "~/.rh": "~/.rh",
        #         }
        #     )
        # If we choose to reduce collisions of cluster names:
        # cluster_name = self.rns_address.strip('~/').replace("/", "-")
        backend = CloudVmRayBackendAirExtend(self.log)

        sky.launch(
            task,
            backend = backend,
            cluster_name="testtesttest",
            stream_logs = True,
            idle_minutes_to_autostop=5,
            down=True,
        )