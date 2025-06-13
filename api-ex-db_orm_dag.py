import requests
import json
import pytz

from datetime import datetime
from airflow.sdk import DAG, task, Variable # type: ignore

from sqlalchemy import create_engine, Column, Integer, String, text # type: ignore
from sqlalchemy.ext.declarative import declarative_base # type: ignore
from sqlalchemy.orm import sessionmaker # type: ignore

# определяем DAG
with DAG(
    dag_id='api-ex-db_orm_dag', 
    start_date=datetime(2025, 1, 1),
    schedule="0 0 * * *"
) as dag:

    # таск для получения данных из api погоды
    @task
    def get_data():
        # берем значения из вариаблес
        api_key = Variable.get("API_KEY")
        url = Variable.get("URL")

        response = requests.get(f"{url}{api_key}")

        # проверка подключения
        if response.status_code == 200:
            json_data = json.dumps(response.json(), indent=4, ensure_ascii=False)
            print("conn est")
        else:
            print("bad conn")

        # переделываем данные в дикт
        data = json.loads(json_data)
        print(data)
        return data

    # таск для вытаскивания данных из дикта в отдельные переменные
    @task
    def extract_data(**context):
        # передача через xcom
        ti = context['ti']
        data = ti.xcom_pull(task_ids='get_data')
    
        city_name = data['name']
        weather_desc = data['weather'][0]['description']
        temp_f = data['main']['temp']
        temp_c = round(temp_f - 273.15, 2)
        pressure = data['main']['pressure']
        humidity = data['main']['humidity']
        wind_speed = data['wind']['speed']

        # время в лондоне (utc)
        tz = pytz.timezone('Europe/London')
        date = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

        print("city -", city_name)
        print("date - ", date)
        print("weather desc -", weather_desc)
        print("temp c -", temp_c)
        print("pressure -", pressure)
        print("humidity -", humidity)
        print("wind speed -", wind_speed)
        
        return city_name, date, weather_desc, temp_c, pressure, humidity, wind_speed

    # таска для вставки переменных в бд
    @task
    def insert_data(**context):
        # передача через xcom
        ti = context['ti']
        city_name, date, weather_desc, temp_c, pressure, humidity, wind_speed = ti.xcom_pull(task_ids='extract_data')

        db_url = Variable.get("POSTGRES_URL")
        engine = create_engine(db_url)
        Base = declarative_base()
        Session = sessionmaker(bind=engine)
        session = Session()

        # создаем тейбл с колоннами
        class Weather(Base):
            __tablename__ = "weather_london_orm"

            id = Column(Integer, primary_key=True, autoincrement=True)
            date = Column(String(30))
            city = Column(String(20))
            wdesc = Column(String(30))
            temp = Column(Integer)
            pressure = Column(Integer)
            humidity = Column(Integer)
            wspeed = Column(Integer)

            # чтобы лучше видеть в логах
            def __repr__(self):
                return f"""
##################################################################################                
id - {self.id}, date - {self.date}, city - {self.city}, 
weather_desc - {self.wdesc}, temp - {self.temp}, pressure - {self.pressure}, 
humidity - {self.humidity}, wind_speed - {self.wspeed}
##################################################################################"""

        # делаем таблицу
        Base.metadata.create_all(engine)

        # для вставки данных
        def insertdata():
            weather1 = Weather(
                date = date,
                city = city_name,
                wdesc = weather_desc,
                temp = temp_c,
                pressure = pressure,
                humidity = humidity,
                wspeed = wind_speed,
            )

            session.add_all([weather1])
            session.commit()

        # для вывода данных в логи
        def getdata():
            print(f"\nWeather: \n")
            for weather in session.query(Weather).all():
                print(weather)
        
        # осторожно! очистить тейбл
        def deldata():
            with engine.connect() as conn:
                conn.execute(text("TRUNCATE TABLE weather_london_orm"))

        #insertdata()                   
        #deldata()
        getdata()

    get_data() >> extract_data() >> insert_data()