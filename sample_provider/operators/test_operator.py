from __future__ import annotations

import logging
import os

from typing import Any, TYPE_CHECKING


from airflow.models import BaseOperator


import sky
from sky.cli import _make_task_or_dag_from_entrypoint_with_overrides
from sky import global_user_state

from sample_provider.skycore.sky_bash_cmd import BashCmd
from sample_provider.skycore.sky_core import CloudVmRayBackendAirExtend
from sample_provider.skycore.sky_log import SkyLogFilter

if TYPE_CHECKING:
    from airflow.utils.context import Context

class SkyOperator(BaseOperator):
    ui_color = "#82A8DC"
    def __init__(
            self,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.home_dir = None
        self.pre_cwd = None

    def execute(self, context: Context) -> Any:

        self.home_dir = os.path.expanduser('~')
        self.pre_cwd = os.getcwd()
        os.chdir(self.home_dir)
        self._add_sky_format_logger()

        result = self._sky_execute(context)

        self._remove_sky_format_log_handler()
        os.chdir(self.pre_cwd)
        return result

    def _sky_execute(self, context: Context) -> Any:
        raise NotImplementedError("_sky_execute must be implemented by SkyOperator subclasses")

    def _add_sky_format_logger(self):
        sky_filter: SkyLogFilter | None = None

        for f in self.log.filters:
            if isinstance(f, SkyLogFilter):
                sky_filter = f
                break

        if sky_filter is None:
            self.log.addFilter(SkyLogFilter())

        from sky import optimizer
        from sky.provision import provisioner
        from sky.backends import cloud_vm_ray_backend

        optimizer.logger = self.log
        provisioner.logger = self.log
        cloud_vm_ray_backend.logger = self.log

    def _remove_sky_format_log_handler(self):
        sky_filters = []
        for h in self.log.filters:
            if isinstance(h, SkyLogFilter):
                sky_filters.append(h)

        for h in sky_filters:
            self.log.removeFilter(h)

class SkyLaunchOperator(SkyOperator):

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

    def cp_env_dir(self) -> None:
        for item in os.listdir(self.sky_working_dir):
            src = os.path.join(self.sky_working_dir, item)
            dest = os.path.join(self.home_dir, item)
            if os.path.exists(dest): continue
            os.symlink(src, dest)

    def _sky_execute(self, context: Context) -> Any:
        self.cp_env_dir()

        check_cmd = BashCmd(bash_command="sky check")
        check_cmd.bash_execute(context)

        enabled_clouds = global_user_state.get_enabled_clouds()
        if len(enabled_clouds) == 0: self.end_exec()

        cluster = self.launch()

        if self.auto_down:
            sky.down(cluster)
            self.log.info(f'Cluster {cluster} Terminated.')

        self.log.info("Done")
        return cluster


    def launch(self):

        self.provider = 'cheapest'
        self.num_instances = 1
        task = _make_task_or_dag_from_entrypoint_with_overrides(entrypoint=[self.sky_task_yaml], num_nodes=2)

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
            cluster_name="node2test",
            stream_logs = True,
            idle_minutes_to_autostop=5,
            down=True,
        )

