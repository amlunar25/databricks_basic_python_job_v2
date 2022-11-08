#! /bin/bash -e
#Creating Virtual Environment


echo "Create a new conda environment 'databricks-local' with required dependencies? (y/n)"
read CREATE_CONDA_ENV

if [ $CREATE_CONDA_ENV == 'y' ] || [ $CREATE_CONDA_ENV == 'Y' ]; then
    echo "--------CREATING NEW CONDA ENVIRONMENT--------"
    CONDA_ENV="databricks-local"
    conda create -y -n "$CONDA_ENV" python=3.9
    conda install -c conda-forge openjdk=11.0.15
else
    echo "Input conda environment name you'd like use from prior bootstrap:"
    read CONDA_ENV
fi

## Upgrade pip
eval "$(conda shell.bash hook)"
conda activate $CONDA_ENV
echo "---------UDPATING PIP AND SETUP TOOLS in $CONDA_ENV --------------"
pip install --upgrade pip
pip install setuptools build

echo "---------INSTALLING PROJECT TOOLS in $CONDA_ENV --------------"

pip install dbx
pip install -e ".[local,test]"

export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=python