FONDA Airflow Dags
==============
This repository contains [Apache Airflow](https://airflow.apache.org/) DAGs used in the FONDA project. 
The DAGs are located in `./dags`, currently organized in subfolders based on FONDA subprojects and regulary synchronized with the Airflow instance running in the reference stack.

To add a DAG, create a branch and use the development environment as described below. When you're done writing your DAG, you can create a Pull Request and merge to the main branch.

TODO:
- [ ] integrate repository into Jenkins CI and apply testing/static-code-analysis before merging
- [ ] synchronize other DAG repositories with reference stack 

#### Development environment
We provide scripts to create a local development environment based on [Kubernetes in Docker](https://github.com/kubernetes-sigs/kind) which can be used to develop Airflow DAGs in a similar environment to the FONDA reference stack. Kubernetes will be deployed in Docker containers on your local machine and you can use the [KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html) to create DAGs.

##### Requirements
* Git
* [Docker](https://docs.docker.com/get-docker/)
* [kubectl](https://kubernetes.io/docs/tasks/tools/)
* [helm](https://helm.sh/docs/intro/install/)

##### Installation
* `./kind/start-dev-env.sh`
* `./kind/create-admin-user.sh`

Open http://localhost:8888 in your browser. The `dags` folder is available in Airflow and you can start developing your DAG.

##### Delete development environment
* `./kind/stop-dev-env.sh`