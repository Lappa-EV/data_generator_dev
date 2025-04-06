from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import random
from datetime import datetime, timedelta
import os
import shutil

# Списки с именами и городами
names = ["Алексей", "Светлана", "Дмитрий", "Екатеринбург", "Иван", "Анна", "Максим", "Ольга", "Анастасия", "Сергей"]
cities = ["Санкт-Петербург", "Новосибирск", "Екатеринбург", "Нижний Новгород", "Челябинск", "Ростов-на-Дону", "Красноярск", "Волгоград", "Самара", "Казань"]

# Функция для генерации данных
def generate_data(num_rows):
    data = []
    for i in range(1, num_rows + 1):  # Начинаем с 1
        name = random.choice(names)
        domain = random.choice(['ru', 'com'])
        email = f"{name.lower()}@example.{domain}"  # Формат email
        city = random.choice([city for city in cities if len(city) >= 7])  # Случайный город с длиной >= 7
        age = random.randint(18, 95)
        salary = random.randint(30000, 150000) if random.random() >= 0.05 else None  # 5% вероятность NULL
        registration_date = (datetime.now() - timedelta(days=age)).date()  # Вычисляем дату регистрации
        data.append(Row(id=i, name=name, email=email, city=city, age=age, salary=salary, registration_date=registration_date))
    return data

# Функция для выполнения генерации данных и их сохранения
def job_generate_and_save(num_rows):
    # Создание Spark сессии
    spark = SparkSession.builder.appName("Synthetic Data Generator").getOrCreate()

    # Определяем схему
    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("email", StringType(), nullable=True),
        StructField("city", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=False),
        StructField("salary", IntegerType(), nullable=True),
        StructField("registration_date", DateType(), nullable=False)
    ])

    # Создаем DataFrame с явным указанием схемы
    df = spark.createDataFrame(generate_data(num_rows), schema=schema)

    # Создаем директорию data, если ее нет
    if not os.path.exists("data"):
        os.makedirs("data")

    output_file = os.path.join("data", f"{datetime.now().strftime('%Y-%m-%d')}-dev.csv")
    df.coalesce(1).write.csv("temp_output", header=True, mode='overwrite')  # Сохраняем DataFrame в одну партицию во временную директорию

    # Переименование и удаление временной директории
    temp_files = os.listdir("temp_output")  # Получаем список файлов данных во временной директории
    os.rename(os.path.join("temp_output", temp_files[0]), output_file)  # Переименовываем и переносим файл с данными
    shutil.rmtree("temp_output")  # Удаляем временную директорию
    print(f"Данные успешно сгенерированы и сохранены в файл: {output_file}")

    # Остановка Spark сессии
    spark.stop()