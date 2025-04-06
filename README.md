# Генератор синтетических данных для наполнения контура DEV

## Описание

Синтетические данные — это искусственно созданные данные, которые имитируют статистические характеристики достоверных данных. В рамках данного задания мы разрабатываем генератор синтетических данных, который будет использоваться для наполнения DEV окружения, необходимого для тестирования аналитических скриптов, работающих с большими данными в PROD среде.

### Задача

Наша задача заключается в генерации синтетических данных в формате CSV, которые соответствуют заданной структуре. Данные необходимы для тестирования скриптов, поскольку копирование данных из PROD в DEV невозможно. 

### Структура данных

Генерируемые данные должны содержать следующие поля:

- **id**: Уникальный идентификатор.
- **name**: Случайное имя (минимум 5 букв).
- **email**: Email, сгенерированный на основе имени (домен должен быть либо .ru, либо .com).
- **city**: Случайный город (минимум 7 букв).
- **age**: Случайный возраст (от 18 до 95 лет).
- **salary**: Случайная зарплата.
- **registration_date**: Дата регистрации, вычисляемая на основе возраста (дата регистрации не должна быть меньше, чем значение в поле age).

### Требования

- Программа должна генерировать один файл формата CSV без партиций и дополнительных success-файлов.
- При запуске программы необходимо ввести количество генерируемых строк (можно через терминал).
- Используйте PySpark для обработки данных.
- Избегайте использования библиотек, таких как Faker.
- Название выходного файла должно быть в формате `YYYY-MM-DD-dev.csv`.
- Значения NULL допускаются, но не более 5% от общего количества значений в соответствующем столбце.
- При запуске Spark приложения не должно быть ошибок.
- Поставить запуск генерации данных на ежедневное выполнение.

## Установка и запуск

1. Убедитесь, что у вас установлены необходимые зависимости, включая PySpark.
2. Сохраните файлы `data_generator.py` и `scheduler.py` в одной директории.
3. Запустите `scheduler.py`, который будет выполнять генерацию данных:

```bash
python scheduler.py
```

4. При первом запуске введите количество строк, которые необходимо сгенерировать.

## Структура файлов

### data_generator.py

Этот файл отвечает за генерацию синтетических данных. Он включает в себя функции для создания данных и их сохранения в CSV файл.

### scheduler.py

Этот файл управляет процессом генерации данных. Он запрашивает количество строк для генерации и запускает процесс генерации с заданным интервалом (каждые 24 часа).

## Пример использования

После запуска `scheduler.py` программа запросит у вас количество строк для генерации. После этого будет создан CSV файл с синтетическими данными, который можно использовать для тестирования в DEV окружении.

## Заключение

Данный генератор синтетических данных поможет быстро и эффективно подготовить тестовые данные для разработки и тестирования аналитических скриптов.
