from __future__ import annotations

import getpass
import os.path
import re
import sys
from typing import Mapping, Any, TYPE_CHECKING

from airflow.models import BaseOperator
from sky import core, ClusterStatus
from sky.backends.backend_utils import get_cleaned_username
from sky.cli import _get_glob_clusters, down

from sample_provider.operators.bash_cmd import BashCmd


from sample_provider.operators.utils import RedirectPrinter

if TYPE_CHECKING:
    from airflow.utils.context import Context

class SkyOperator(BaseOperator):
    template_fields = [
        "op_option_list"
    ]
    template_fields_renderers = {"op_option_list": "py"}
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        sky_task_yaml: str,
        op_option_list: str | None = None,
        sky_working_dir: str = '/opt/airflow/sky_workdir',
        **kwargs
    ) -> None :
        super().__init__(**kwargs)
        self.cmd_lines = [sky_task_yaml]
        self.op_option_list = op_option_list or []

        if '-y' not in self.op_option_list and '--yes' not in self.op_option_list: self.op_option_list.append("--yes")
        self.cmd_lines.extend(self.op_option_list)
        self.sky_working_dir = sky_working_dir
        self.home_dir = os.path.expanduser('~')
        os.environ['SKYPILOT_MINIMIZE_LOGGING'] = '1'


    def cp_env_dir(self) -> None:

        for item in os.listdir(self.sky_working_dir):
            src = os.path.join(self.sky_working_dir, item)
            dest = os.path.join(self.home_dir, item)
            if os.path.exists(dest): continue
            os.symlink(src, dest)

    def execute(self, context:Context) -> Any:

        self.pre_cwd = os.getcwd()
        os.chdir(self.home_dir)
        self.cp_env_dir()


        check_cmd = BashCmd(bash_command = "sky check")
        check_cmd.bash_execute(context)
        from sky import global_user_state
        enabled_clouds = global_user_state.get_enabled_clouds()
        if len(enabled_clouds) == 0: self.end_exec()

        cluster = self.launch(context)

        try: down([cluster, '-y'])
        except SystemExit: pass

        self.log.info("Done")
        self.end_exec()
        return cluster

    def end_exec(self):
        os.chdir(self.pre_cwd)
        return

    def launch(self, context):
        launch_cnd = "sky launch " + " ".join((self.cmd_lines))
        check_cmd = BashCmd(bash_command=launch_cnd)
        line_query ="Running task on cluster"
        line_captured = check_cmd.bash_exec_and_get_matched_line(context, [line_query])
        line_captured = line_captured[line_query][0]

        username = get_cleaned_username()
        cluster_name_pattern = re.compile(r'sky-[0-9a-fA-F]{4}-'+username)
        cluster_name = re.search(cluster_name_pattern, line_captured).group()

        query_clusters = _get_glob_clusters([cluster_name], silent=True)
        cluster_recoreds = core.status(cluster_names=query_clusters, refresh=False)
        cluster_recored = cluster_recoreds[0]
        status = cluster_recored['status']
        assert status == ClusterStatus.UP, "Cluster is not up"

        return cluster_name



