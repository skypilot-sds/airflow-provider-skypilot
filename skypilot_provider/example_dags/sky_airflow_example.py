import datetime

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from skypilot_provider.operators import *


with DAG(
    dag_id = "sky_airflow_example",
    schedule="0 0 1 1 *",
    start_date = pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
) as dag:

    git_clone_or_update_cmd = """
export SKY_WORK_DIR=/opt/airflow/sky_home_dir/sky_workdir
git -C $SKY_WORK_DIR/torch_examples pull \
|| git clone https://github.com/pytorch/examples.git $SKY_WORK_DIR/torch_examples
"""
    git_clone_task = BashOperator(
        task_id='git_clone_task',
        bash_command=git_clone_or_update_cmd,
        dag=dag
    )

    make_yaml_cmd = """
export SKY_WORK_DIR=/opt/airflow/sky_home_dir/sky_workdir
cat > $SKY_WORK_DIR/sky_task.yaml << EOF
workdir: ~/sky_workdir/torch_examples
setup: |
  pip install torch torchvision
run: |
  cd mnist
  python main.py --epochs 1 --save-model
EOF
"""
    make_yaml_task = BashOperator(
        task_id='make_yaml_task',
        bash_command=make_yaml_cmd,
        dag=dag
    )

    sky_launch_task = SkyLaunchOperator(
        task_id="sky_launch_task",
        sky_task_yaml="~/sky_workdir/sky_task.yaml",
        cloud="cheapest",
        minimum_cpus=2,
        minimum_memory=2,
        disk_size=100,
        auto_down=False,
        dag=dag
    )

    sky_exec_task = SkyExecOperator(
        task_id="sky_exec_task",
        cluster_name=sky_launch_task.output,
        sky_task_yaml="~/sky_workdir/sky_task.yaml",
        dag=dag
    )

    sky_ssh_task = SkySSHOperator(
        task_id="sky_ssh_task",
        cluster_name=sky_exec_task.output,
        command="cp ~/sky_workdir/mnist/mnist_cnn.pt ~/mnist_cnn_{{ds}}.pt"
    )

    sky_rsync_down_task = SkyRsyncDownOperator(
        task_id="sky_rsync_down_task",
        cluster_name=sky_exec_task.output,
        source='~/mnist_cnn_{{ds}}.pt',
        target='/opt/airflow/sky_home_dir/sky_workdir/',
    )

    sky_vm_down_task = SkyDownOperator(
        task_id="sky_vm_down_task",
        cluster_name=sky_rsync_down_task.output
    )

    git_clone_task >> sky_launch_task
    make_yaml_task >> sky_launch_task
    sky_ssh_task >> sky_rsync_down_task





