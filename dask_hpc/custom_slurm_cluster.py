import getpass
import os
import socket
from enum import Enum

from dask_jobqueue import SLURMCluster
from dask.distributed import Client


class EnvironmentType(Enum):
    POETRY = "poetry"
    CONDA = "conda"


class CustomSLURMCluster(SLURMCluster):
    def __init__(
        self,
        account: str,
        project_dir: str,
        env_type: EnvironmentType = EnvironmentType.POETRY,
        conda_env_path: str = None,
        cores: int = 1,
        processes: int = 1,
        memory: str = "10GB",
        login_node: str = "ares.cyfronet.pl",
        queue: str = "plgrid",
        walltime: str = "02:00:00",
        local_directory: str = None,
        log_directory: str = None,
        silence_logs: str = "info",
        **kwargs,
    ):
        """
        Initializes the CustomSLURMCluster with the given parameters.

        Args:
            account (str): SLURM account name.
            project_dir (str): Path to the project directory.
            env_type (EnvironmentType, optional): Environment type to use: EnvironmentType.POETRY or EnvironmentType.CONDA.
            conda_env_path (str, optional): Path to the .sqsh conda environment. Required if env_type is CONDA.
            cores (int, optional): Number of CPU cores per job. Defaults to 1.
            processes (int, optional): Number of processes per job. Defaults to 1.
            memory (str, optional): Memory allocated per job. Defaults to "10GB".
            login_node (str, optional): Login node for job submission. Defaults to "ares.cyfronet.pl".
            queue (str, optional): SLURM queue name. Defaults to "plgrid".
            walltime (str, optional): Maximum execution time for jobs. Defaults to "02:00:00".
            local_directory (str, optional): Directory for temporary files. Defaults to `SCRATCH/dask_tmp`.
            log_directory (str, optional): Directory for logs. Defaults to `SCRATCH/dask_logs`.
            silence_logs (str, optional): Logging level. Defaults to "info".
            **kwargs: Additional keyword arguments for SLURMCluster.
        """

        self.scratch = os.environ.get("SCRATCH", "")
        self.local_directory = local_directory or f"{self.scratch}/dask_tmp"
        self.log_directory = log_directory or f"{self.scratch}/dask_logs"

        self.plg_user = getpass.getuser()
        self.login_node = login_node

        # Define job submission command via SSH to the login node (sbatch is not available on the compute nodes)
        self.job_cls.submit_command = f"ssh {self.plg_user}@{self.login_node} sbatch"

        # Setup environment (Poetry or Conda)
        job_script_prologue = self._setup_environment(env_type, project_dir, conda_env_path)

        super().__init__(
            cores=cores,
            memory=memory,
            processes=processes,
            account=account,
            queue=queue,
            interface="ib0",
            walltime=walltime,
            local_directory=self.local_directory,
            log_directory=self.log_directory,
            silence_logs=silence_logs,
            python="python",
            job_extra_directives=["-C memfs"],
            job_script_prologue=job_script_prologue,
            **kwargs,
        )

    def _setup_environment(self, env_type: EnvironmentType, project_dir: str, conda_env_path: str):
        if env_type == EnvironmentType.POETRY:
            resolved_project_dir = os.path.expandvars(project_dir)
            pyproject_path = os.path.join(resolved_project_dir, "pyproject.toml")
            if not os.path.isfile(pyproject_path):
                raise FileNotFoundError(f"pyproject.toml not found in {project_dir}")
            return self._setup_poetry_environment(project_dir)

        elif env_type == EnvironmentType.CONDA:
            if not conda_env_path:
                raise ValueError("conda_env_path must be provided when env_type is CONDA")
            if not os.path.isfile(conda_env_path):
                raise FileNotFoundError(f"Conda .sqsh file not found at {conda_env_path}")
            return self._setup_conda_environment(conda_env_path)

        else:
            raise ValueError("env_type must be EnvironmentType.POETRY or EnvironmentType.CONDA")

    def _setup_poetry_environment(self, project_dir: str):
        project_name = os.path.basename(project_dir)
        return [
            "module load poetry/1.8.3-gcccore-12.3.0",
            "export POETRY_CONFIG_DIR=$MEMFS/poetry_config",
            "export POETRY_DATA_DIR=$MEMFS/poetry_data",
            "export POETRY_CACHE_DIR=$MEMFS/poetry_cache",
            "export POETRY_VIRTUALENVS_IN_PROJECT=false",
            f"cp -r {project_dir} $MEMFS",
            f"cd $MEMFS/{project_name}",
            "poetry install --with=dev",
            "source $(poetry env info --path)/bin/activate",
        ]

    def _setup_conda_environment(self, conda_env_path: str):
        return [
            "module load miniconda3",
            f"SRC_PATH={conda_env_path}",
            "DEST_PATH=$MEMFS/lgad.sqsh",
            "ENV_PATH=$MEMFS/envs/lgad",
            "cp $SRC_PATH $DEST_PATH",
            "mkdir -p $ENV_PATH",
            "squashfuse $DEST_PATH $ENV_PATH",
            "eval $(conda shell.bash hook)",
            "conda activate $ENV_PATH",
            "conda config --append envs_dirs $MEMFS/envs",
            "export LD_LIBRARY_PATH=$CONDA_PREFIX/lib:$LD_LIBRARY_PATH",
        ]

    @property
    def client(self):
        """
        Returns a Dask Client connected to the cluster.

        Returns:
            Client: Dask client instance.
        """
        return Client(self)

    def get_dashboard_info(self):
        """
        Displays instructions for tunneling to the Dask dashboard.

        Prints SSH command to tunnel and access the dashboard.
        """
        host = self.client.run_on_scheduler(socket.gethostname)
        port = self.client.scheduler_info().get("services", {}).get("dashboard", 8787)
        login_node_address = f"{self.plg_user}@{self.login_node}"

        print("To access the Dask dashboard, run the following command:")
        print(f"ssh -L {port}:{host}:{port} {login_node_address}")
        print(f"Then open http://localhost:{port} in your browser.")
