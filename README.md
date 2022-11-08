![Proactiviti](https://lucid.app/publicSegments/view/687883a3-5f2b-43a3-b91e-ffd1f1fdc977/image.png)
# Databricks Basic Python Job

This is a sample code base to fork and begin development for Proactivti Data Engineering projects that includes basic unit tests, integration tests, and automated deployments.

While using this code, you need:
- Python 3.X
- Databricks CLI
- Databricks Access Token
- Git installed on your local environment.
- Windows Users: Install [Windows Subsystem for Linux (WSL)](https://code.visualstudio.com/docs/remote/wsl)
- Optional: Visual Studio Code.

## Local environment setup

1. You need to fork this repository. If you have cloned this repository, you have made a mistake. This should be a starting point for your jobs. [How to Fork a Repository on Github] (https://docs.github.com/en/get-started/quickstart/fork-a-repo)

2. Run `bootstrap.sh` to install needed dependencies. This will work in a linux based environment to run bash on.
```bash
bootstrap.sh
```

3. Ensure your virtural environment is activated. You will see `(.env)` on the terminal if it is. To activate you virtual environment:
```bash
source .venv/bin/activate
```

_To learn more about virtual environments, read [this blog post](https://towardsdatascience.com/why-you-should-use-a-virtual-environment-for-every-python-project-c17dab3b0fd0)_

4. You should be ready to begin development.

## Running unit tests

For unit testing, please use `pytest`:
```
pytest tests/unit --cov
```

Please check the directory `tests/unit` for more details on how to use unit tests.
In the `tests/unit/conftest.py` you'll also find useful testing primitives, such as local Spark instance with Delta support, local MLflow and DBUtils fixture. **Do not modify these**

## Running integration tests

There are two options for running integration tests on Databricks clusters:

- On an all-purpose cluster via `dbx execute`
- On a job cluster via `dbx launch`

For quicker startup of the job clusters we recommend using instance pools [Azure](https://docs.microsoft.com/en-us/azure/databricks/clusters/instance-pools/)

For an integration test on all-purpose cluster, use the following command:
```
dbx execute <workflow-name> --cluster-name=<name of all-purpose cluster>
```

To execute a task inside multitask job, use the following command:
```
dbx execute <workflow-name> \
    --cluster-name=<name of all-purpose cluster> \
    --job=<name of the job to test> \
    --task=<task-key-from-job-definition>
```
_`dbx execute` is similar to spark-submit in classic Apache Spark. 

For a test on a job cluster, deploy the job assets and then launch a run from them:
```
dbx deploy <workflow-name> --assets-only
dbx launch <workflow-name>  --from-assets --trace
```


## Interactive execution and development on Databricks clusters

1. `dbx` expects that cluster for interactive execution supports `%pip` and `%conda` magic [commands](https://docs.databricks.com/libraries/notebooks-python-libraries.html).
2. Please configure your workflow (and tasks inside it) in `conf/deployment.yml` file.
3. To execute the code interactively, provide either `--cluster-id` or `--cluster-name`.
```bash
dbx execute <workflow-name> \
    --cluster-name="<some-cluster-name>"
```

Multiple users also can use the same cluster for development. Libraries will be isolated per each user execution context.

## Working with Notebooks

Notebooks are great prototyping tools that allow for interactive troubleshooting, and data exploration. They should be used for just those tasks. Notebooks are not production-ready software, and should not be treated like such. While you may start your development in a notebook, they should be moved to a production ready package in a code base this this repository.

## CI/CD pipeline settings

You will need to set the following secrets for the CI pipeline to work appropriately. For Github you can find how to create/update secrets [at this link Github Encrypted Secrets](https://docs.github.com/en/actions/security-guides/encrypted-secrets)
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

## Testing and releasing via CI pipeline

- To trigger the CI pipeline, simply push your code to the repository. If CI provider is correctly set, it shall trigger the general testing pipeline
- To trigger the release pipeline, get the current version from the `basic_python_job/__init__.py` file and tag the current code version:
```
git tag -a v<your-project-version> -m "Release tag for version <your-project-version>"
git push origin --tags
```
