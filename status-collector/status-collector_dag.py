from datetime import datetime
import requests

from airflow.sdk import DAG, task, Variable  # type: ignore

# Определяем DAG
with DAG(
    dag_id='status-collector_dag', 
    start_date=datetime(2025, 1, 1),
    schedule = "@daily"
) as dag:
    
    # таск для сбора статуса с ссылок
    @task
    def collect_status():

        # лист ссылок
        urls = ["https://gitlab.com/", "https://github.com/", "https://www.google.com/"]

        report = []

        # проходим по каждой ссылке и проверяем код
        for url in urls:
            response = requests.get(url)
            if response.status_code == 200:
                print(f"conn with {url} est, code - {response.status_code}")
                report.append(f"conn with {url} est, code - {response.status_code}")
            else:
                print(f"bad conn with {url}, code - {response.status_code}")
                report.append(f"bad conn with {url}, code - {response.status_code}")

        print("##############################################################")
        print("\n".join(report))
        print("##############################################################")

        return "\n".join(report)

    # таска дял отправки репорта по тг
    @task   
    def send_to_tg_py(**context):
        ti = context['ti']
        text = ti.xcom_pull(task_ids='collect_status')

        # ставим свои переменные
        token = Variable.get("WATER_BOT_TOKEN")
        chat_id = Variable.get("MY_TG_ID")
        
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        params = {
            "chat_id": chat_id,
            "text": text
        }

        response = requests.post(url, params=params)
        response.raise_for_status()

    collect_status() >> send_to_tg_py()

   
