from __future__ import annotations

import getpass
import os.path
import sys
from typing import Mapping, Any, TYPE_CHECKING

from airflow.models import BaseOperator

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
        sky_working_dir: str = '/opt/airflow/sky_working_dir',
        **kwargs
    ) -> None :
        super().__init__(**kwargs)
        self.cmd_lines = [sky_task_yaml]
        self.op_option_list = op_option_list or []

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

    def execute(self, context:Context) -> Any:
        print("!!!!!!!!!!!!!!!!!!", self.home_dir)
        print("!!!!!!!!!!!!!!!!!!", getpass.getuser())

        self.pre_cwd = os.getcwd()
        os.chdir(self.home_dir)
        self.cp_env_dir()


        check_cmd = BashCmd(bash_command = "sky check")
        check_cmd.bash_execute(context)

        # launch_cnd = "sky launch " + " ".join((self.cmd_lines))
        # check_cmd = BashCmd(bash_command=launch_cnd)
        # check_cmd.bash_execute(context)



        sys.stdout = RedirectBuffer(sys.stdout)

        from sky import global_user_state
        from sky.cli import launch
        enabled_clouds = global_user_state.get_enabled_clouds()
        if len(enabled_clouds) == 0: self.end_exec()







        try:
            # with RedirectPrinter():
            launch(self.cmd_lines)

        except SystemExit as e:
            self.log.info("EXIT")
        self.log.info("Done")
        self.end_exec()

    def end_exec(self):
        os.chdir(self.pre_cwd)
        return


class RedirectBuffer:
    def __init__(self, oldout):
        self.out = oldout
    def write(self, msg):
        msgs = msg.split
        self.out.write("!!!!!!"+msg)
    def flush(self):
        self.out.flush()