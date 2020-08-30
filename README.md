# PysparkProductionReady

<p align="center"> 
<img src="images/PysparkProductionReady.png">
</p>



docker-compose up -d

ubuntu_container is needed only for Windows users

Submit spark job to Spark in Jupyter:
    cd scripts && spark-submit --py-files jobs.zip main.py --job test_submit
    docker exec -w /home/jovyan/scripts jupyter_spark spark-submit --py-files jobs.zip main.py --job test_submit

Make:
    make build