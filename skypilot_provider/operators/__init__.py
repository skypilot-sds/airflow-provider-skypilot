import os


from skypilot_provider.operators.core_function import SkyLaunchOperator, SkyExecOperator, SkyDownOperator
from skypilot_provider.operators.rsync import SkyRsyncUpOperator, SkyRsyncDownOperator
from skypilot_provider.operators.ssh import SkySSHOperator

os.environ['SKYPILOT_MINIMIZE_LOGGING'] = '1'
DEFAULT_SKY_HOME = '/opt/airflow/sky_home_dir'
__all__ = [
    'SkyLaunchOperator',
    'SkyExecOperator',
    'SkyDownOperator',
    'SkyRsyncUpOperator',
    'SkyRsyncDownOperator',
    'SkySSHOperator'
]
