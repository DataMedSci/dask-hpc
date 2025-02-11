#!/bin/bash
#SBATCH --job-name="vscode"
#SBATCH --time=4:00:00     # walltime
#SBATCH -C memfs

project_dir=$1

module load poetry/1.8.3-gcccore-12.3.0


if [ -n "$MEMFS" ]; then
    cd $project_dir

    export POETRY_CONFIG_DIR="$MEMFS/poetry_config"
    export POETRY_DATA_DIR="$MEMFS/poetry_data"
    export POETRY_CACHE_DIR="$MEMFS/poetry_cache"
    export POETRY_VIRTUALENVS_IN_PROJECT=false

    poetry install --with=dev
    source $(poetry env info --path)/bin/activate
else
    echo "MEMFS is not set."
fi

# install VSCode and start the tunnel
cd ${MEMFS}
curl -Lk 'https://code.visualstudio.com/sha/download?build=stable&os=cli-alpine-x64' --output vscode_cli.tar.gz
tar -xf vscode_cli.tar.gz
./code tunnel  --accept-server-license-terms --name $HOSTNAME
