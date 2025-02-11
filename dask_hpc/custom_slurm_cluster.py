import getpass
import os
import socket

from dask_jobqueue import SLURMCluster
from dask.distributed import Client


class CustomSLURMCluster(SLURMCluster):
    def __init__(
        self,
        account: str,
        project_dir: str,
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

        # Setup Python environment with Poetry
        job_script_prologue = self._setup_poetry_environment(project_dir)

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

    def _setup_poetry_environment(self, project_dir: str):
        """
        Configures the Poetry environment in MEMFS.

        Args:
            project_dir (str): Path to the project directory.

        Returns:
            list: List of commands to set up Poetry.
        """
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
