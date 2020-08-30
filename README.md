# PysparkProductionReady

<p align="center"> 
<img src="images/PysparkProductionReady.png">
</p>

Folders:

    - ./movies_ratings: pyspark code and Makefile
    - ./notebooks: Jupyter Notebooks
    - ./hdfs: volume used by locally Apache Spark
    - ./ubuntu: Ubuntu Dockerfile


Make:

    - make test: run pytest tests
    - make build: build jobs to ./dist folder
    - make all: test and build


Local environment:

    - Apache Spark on JupyterNotebook docker container
    - docker-compose up -d
    - ubuntu_container is needed only for Windows users. To run "make" commands


Submit job to Spark on Jupyter:

    - docker exec -w /home/jovyan/scripts jupyter_spark spark-submit --py-files jobs.zip main.py --job test_submit
