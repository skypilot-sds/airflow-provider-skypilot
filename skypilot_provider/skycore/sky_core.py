"""Backend: runs on cloud virtual machines, managed by Ray.

 Modified several methods of sky.backends.CloudVmRayBackend, to print the outputs to Airflow logs.
 """

import os
import subprocess
import tempfile
import time
from typing import  List, Optional, Tuple, Union

import colorama
from sky import task as task_lib, global_user_state
from sky.backends import backend_utils, CloudVmRayBackend, CloudVmRayResourceHandle
from sky.backends.backend_utils import SKY_USER_FILE_PATH
from sky.skylet import constants
from sky.skylet import log_lib
from sky.utils import command_runner
from sky.utils import subprocess_utils
from sky.utils import timeline
from sky import resources as resources_lib


_FETCH_IP_MAX_ATTEMPTS = 3
SKY_REMOTE_WORKDIR = constants.SKY_REMOTE_WORKDIR

logger = None


class CloudVmRayBackendAirExtend(CloudVmRayBackend):

    def __init__(self, log):
        super().__init__()
        global logger
        logger = log

    def _setup(self, handle: CloudVmRayResourceHandle, task: task_lib.Task,
               detach_setup: bool) -> None:
        start = time.time()
        style = colorama.Style
        fore = colorama.Fore

        if task.setup is None:
            return

        setup_script = log_lib.make_task_bash_script(task.setup,
                                                     env_vars=task.envs)
        with tempfile.NamedTemporaryFile('w', prefix='sky_setup_') as f:
            f.write(setup_script)
            f.flush()
            setup_sh_path = f.name
            setup_file = os.path.basename(setup_sh_path)
            # Sync the setup script up and run it.
            ip_list = handle.external_ips()
            port_list = handle.external_ssh_ports()
            assert ip_list is not None, 'external_ips is not cached in handle'
            ssh_credentials = backend_utils.ssh_credential_from_yaml(
                handle.cluster_yaml, handle.docker_user)
            # Disable connection sharing for setup script to avoid old
            # connections being reused, which may cause stale ssh agent
            # forwarding.
            ssh_credentials.pop('ssh_control_name')
            runners = command_runner.SSHCommandRunner.make_runner_list(
                ip_list, port_list=port_list, **ssh_credentials)

            # Need this `-i` option to make sure `source ~/.bashrc` work
            setup_cmd = f'/bin/bash -i /tmp/{setup_file} 2>&1'

            def _setup_node(runner: command_runner.SSHCommandRunner) -> None:
                runner.rsync(source=setup_sh_path,
                             target=f'/tmp/{setup_file}',
                             up=True,
                             stream_logs=False)
                if detach_setup:
                    return
                setup_log_path = os.path.join(self.log_dir,
                                              f'setup-{runner.ip}.log')
                returncode = runner.run(
                    setup_cmd,
                    log_path=setup_log_path,
                    process_stream=True,
                )

                def error_message() -> str:
                    # Use the function to avoid tailing the file in success case
                    try:
                        last_10_lines = subprocess.run(
                            [
                                'tail', '-n10',
                                os.path.expanduser(setup_log_path)
                            ],
                            stdout=subprocess.PIPE,
                            check=True).stdout.decode('utf-8')
                    except subprocess.CalledProcessError:
                        last_10_lines = None

                    err_msg = (
                        f'Failed to setup with return code {returncode}. '
                        f'Check the details in log: {setup_log_path}')
                    if last_10_lines:
                        err_msg += (
                            f'\n\n{colorama.Fore.RED}'
                            '****** START Last lines of setup output ******'
                            f'{colorama.Style.RESET_ALL}\n'
                            f'{last_10_lines}'
                            f'{colorama.Fore.RED}'
                            '******* END Last lines of setup output *******'
                            f'{colorama.Style.RESET_ALL}')
                    return err_msg

                subprocess_utils.handle_returncode(returncode=returncode,
                                                   command=setup_cmd,
                                                   error_msg=error_message)

            num_nodes = len(ip_list)
            plural = 's' if num_nodes > 1 else ''
            if not detach_setup:
                logger.info(
                    f'{fore.CYAN}Running setup on {num_nodes} node{plural}.'
                    f'{style.RESET_ALL}')
            subprocess_utils.run_in_parallel(_setup_node, runners)

        if detach_setup:
            # Only set this when setup needs to be run outside the self._setup()
            # as part of a job (--detach-setup).
            self._setup_cmd = setup_cmd
            return
        logger.info(f'{fore.GREEN}Setup completed.{style.RESET_ALL}')
        end = time.time()
        logger.debug(f'Setup took {end - start} seconds.')

    @timeline.event
    def run_on_head(
        self,
        handle: CloudVmRayResourceHandle,
        cmd: str,
        *,
        port_forward: Optional[List[int]] = None,
        log_path: str = '/dev/null',
        stream_logs: bool = False,
        ssh_mode: command_runner.SshMode = command_runner.SshMode.
        NON_INTERACTIVE,
        under_remote_workdir: bool = False,
        require_outputs: bool = False,
        separate_stderr: bool = False,
        process_stream: bool = True,
        **kwargs,
    ) -> Union[int, Tuple[int, str, str]]:

        # This will try to fetch the head node IP if it is not cached.
        stream_logs = True
        process_stream = True
        external_ips = handle.external_ips(max_attempts=_FETCH_IP_MAX_ATTEMPTS)
        head_ip = external_ips[0]
        external_ssh_ports = handle.external_ssh_ports(
            max_attempts=_FETCH_IP_MAX_ATTEMPTS)
        head_ssh_port = external_ssh_ports[0]

        ssh_credentials = backend_utils.ssh_credential_from_yaml(
            handle.cluster_yaml, handle.docker_user)
        runner = command_runner.SSHCommandRunner(head_ip,
                                                 port=head_ssh_port,
                                                 **ssh_credentials)
        if under_remote_workdir:
            cmd = f'cd {SKY_REMOTE_WORKDIR} && {cmd}'

        return runner.run(
            cmd,
            port_forward=port_forward,
            log_path=log_path,
            process_stream=process_stream,
            stream_logs=stream_logs,
            ssh_mode=ssh_mode,
            require_outputs=require_outputs,
            separate_stderr=separate_stderr,
            **kwargs,
        )

    def _provision(
            self,
            task: task_lib.Task,
            to_provision: Optional[resources_lib.Resources],
            dryrun: bool,
            stream_logs: bool,
            cluster_name: str,
            retry_until_up: bool = False) -> Optional[CloudVmRayResourceHandle]:

        handle = super()._provision(task, to_provision, dryrun, stream_logs, cluster_name, retry_until_up)

        cluster_yaml = handle.cluster_yaml
        path, yaml_file_name = os.path.split(cluster_yaml)
        handle._cluster_yaml = os.path.join(SKY_USER_FILE_PATH, yaml_file_name)

        with timeline.Event('backend.provision.post_process'):
            global_user_state.add_or_update_cluster(
                handle.cluster_name,
                handle,
                task.resources,
                ready=True,
            )

        return handle

