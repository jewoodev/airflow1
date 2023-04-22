cd ~
mkdir dags
AIRFLOW_HOME=/var/lib/airflow airflow db init
ls /var/lib/airflow
vim /var/lib/airflow/airflow.cfg
AIRFLOW_HOME=/var/lib/airflow airflow db init
sudo vim /etc/systemd/system/airflow-webserver.service
exit
AIRFLOW_HOME=/var/lib/airflow airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password jewoo4321
cd ~
git clone https://github.com/jewoodev/AirflowRepo.git
cp -r AirflowRepo/* dags
vim ~/.bashrc
source ~/.bashrc
alias
vim ~/.bashrc
exit
cd AirflowRepo
git log --all --oneline
exit
ls
ls AirflowRepo
cd AirflowRepo
git pull
cd ..
cp -r AirflowRepo/* dags
ls dags
ls -l dags
exit
ls
ls -l dags
pwd
exit
cd dags
airflow dags validate flight_data
airflow dags report flight_data
airflow dags report
ls -al
exit
