from __future__ import annotations

import os.path
import re
from typing import Any, TYPE_CHECKING

import paramiko
import sky
from airflow import AirflowException
from airflow.models import BaseOperator
from sky import ClusterStatus
from sky.backends.backend_utils import get_cleaned_username
from sky.utils.command_runner import SSHCommandRunner

from sample_provider.skycore.sky_bash_cmd import BashCmd

if TYPE_CHECKING:
    from airflow.utils.context import Context


def check_available_cluster(cluster_name, available_status_list):
    cluster_recoreds = sky.status(cluster_names=[cluster_name], refresh=True)
    if len(cluster_recoreds) == 0:
        raise AirflowException(f'Cluster {cluster_name} does not exist in SkyPilot DB.')
    cluster_record = cluster_recoreds[0]
    status = cluster_record['status']
    if status not in available_status_list:
        raise AirflowException(f'Status of {cluster_name} should be {str(available_status_list)}, but it is {status}')


class SkyLaunchOperator(BaseOperator):
    template_fields = [
        "op_option_list"
    ]
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
        self.cmd_lines = [sky_task_yaml]
        self.op_option_list = op_option_list or []
        self.auto_down = auto_down

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

    def execute(self, context: Context) -> Any:

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
        launch_cmd = "sky launch " + " ".join((self.cmd_lines))
        launch_cmd = BashCmd(bash_command=launch_cmd)
        line_query = "Running task on cluster"
        line_captured = launch_cmd.bash_exec_and_get_matched_line(context, [line_query])
        line_captured = line_captured[line_query][0]

        username = get_cleaned_username()
        cluster_name_pattern = re.compile(r'sky-[0-9a-fA-F]{4}-' + username)
        cluster_name = re.search(cluster_name_pattern, line_captured).group()

        # query_clusters = _get_glob_clusters([cluster_name], silent=True)
        cluster_recoreds = sky.status(cluster_names=[cluster_name], refresh=False)
        cluster_recored = cluster_recoreds[0]
        status = cluster_recored['status']
        assert status == ClusterStatus.UP, "Cluster is not up"

        return cluster_name


class SkyExecOperator(BaseOperator):
    template_fields = ("cluster_name",)
    ui_color = "#82A8DC"

    def __init__(
            self,
            *,
            cluster_name: str,
            sky_task_yaml: str,
            op_option_list: str | None = None,
            sky_working_dir: str = '/opt/airflow/sky_workdir',
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name

        self.op_option_list = op_option_list or []
        self.sky_task_yaml = os.path.expanduser(sky_task_yaml) if '~' in sky_task_yaml else sky_task_yaml
        self.sky_working_dir = sky_working_dir
        self.home_dir = os.path.expanduser('~')
        os.environ['SKYPILOT_MINIMIZE_LOGGING'] = '1'

    def cp_env_dir(self) -> None:

        for item in os.listdir(self.sky_working_dir):
            src = os.path.join(self.sky_working_dir, item)
            dest = os.path.join(self.home_dir, item)
            if os.path.exists(dest): continue
            os.symlink(src, dest)

    def execute(self, context: Context) -> Any:
        self.cmd_lines = [self.cluster_name, self.sky_task_yaml]
        self.cmd_lines.extend(self.op_option_list)

        self.pre_cwd = os.getcwd()
        os.chdir(self.home_dir)
        self.cp_env_dir()

        from sky import global_user_state
        enabled_clouds = global_user_state.get_enabled_clouds()
        if len(enabled_clouds) == 0: self.end_exec()

        cluster = self.exec(context)

        self.log.info("Done")
        self.end_exec()
        return cluster

    def end_exec(self):
        os.chdir(self.pre_cwd)
        return

    def exec(self, context):
        check_available_cluster(self.cluster_name, [ClusterStatus.UP])

        exec_cmd = "sky exec " + " ".join(self.cmd_lines)
        check_cmd = BashCmd(bash_command=exec_cmd)
        check_cmd.bash_execute(context)

        return self.cluster_name


class SkyRsyncOperator(BaseOperator):
    template_fields = ("cluster_name",)
    ui_color = "#82A8DC"

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

    def execute(self, context: Context) -> Any:
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


class SkyRsyncUpOperator(SkyRsyncOperator):
    def __init__(
            self,
            *,
            cluster_name: str,
            source: str,
            target: str,
            **kwargs
    ) -> None:
        super().__init__(cluster_name=cluster_name, source=source, target=target, up=True, **kwargs)


class SkyRsyncDownOperator(SkyRsyncOperator):
    def __init__(
            self,
            *,
            cluster_name: str,
            source: str,
            target: str,
            **kwargs
    ) -> None:
        super().__init__(cluster_name=cluster_name, source=source, target=target, up=False, **kwargs)


class SkyDownOperator(BaseOperator):
    template_fields = ("cluster_name",)
    ui_color = "#82A8DC"

    def __init__(
            self,
            *,
            cluster_name: str,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name

    def execute(self, context: Context) -> Any:
        check_available_cluster(self.cluster_name, [ClusterStatus.UP, ClusterStatus.INIT, ClusterStatus.STOPPED])
        sky.down(self.cluster_name)
        self.log.info(f'Cluster {self.cluster_name} Terminated.')



