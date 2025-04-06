import time
from data_generator import job_generate_and_save

# Запускаем задачу сразу и потом через каждые 24 ч. (86400 сек.)
if __name__ == "__main__":
    num_rows = int(input("Введите количество строк для генерации: "))  # Ввод количества строк один раз
    job_generate_and_save(num_rows)  # Выполняем задачу сразу при запуске

    while True:
        time.sleep(86400)  # Ждем 24 часа
        job_generate_and_save(num_rows)  # Выполняем задачу снова с тем же количеством строк