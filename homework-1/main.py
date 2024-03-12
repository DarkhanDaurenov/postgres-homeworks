"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv

# Подключение к БД Postgres
conn_params = {
  "host": "localhost",
  "database": "north",
  "user": "postgres",
  "password": "87055797953"
}


#Контекстный менеджер для чтения и встраивания csv файла 'employees_data' в PostgreSQL
try:
    # Открываем файл CSV для чтения через контекстный менеджер
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            with open('north_data/employees_data.csv', mode='r') as file:
                # Создаем объект для чтения CSV-файла
                reader = csv.reader(file)
                #Пропускаем заголовок если он есть
                next(reader, None)
                #Читаем данные из файла и вставляем их в таблицу customers
                for row in reader:
                    cur.execute('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)', row)

    #Коммитим транзакцию
    conn.commit()
    print('Данные усппешно вставлены в БД')
except (Exception, psycopg2.Error) as error:
    print('Ошибка при вставке данных')



#Контекстный менеджер для чтения и встраивания csv файла 'customers_data' в PostgreSQL
try:
    # Открываем файл CSV для чтения через контекстный менеджер
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            with open('north_data/customers_data.csv', mode='r') as file:
                # Создаем объект для чтения CSV-файла
                reader = csv.reader(file)
                #Пропускаем заголовок если он есть
                next(reader, None)
                #Читаем данные из файла и вставляем их в таблицу customers
                for row in reader:
                    cur.execute('INSERT INTO customers VALUES (%s, %s, %s)', row)

    #Коммитим транзакцию
    conn.commit()
    print('Данные усппешно вставлены в БД')
except (Exception, psycopg2.Error) as error:
    print('Ошибка при вставке данных')

#Контекстный менеджер для чтения и встраивания csv файла 'orders_data' в PostgreSQL
try:
    # Открываем файл CSV для чтения через контекстный менеджер
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            with open('north_data/orders_data.csv', mode='r') as file:
                # Создаем объект для чтения CSV-файла
                reader = csv.reader(file)
                #Пропускаем заголовок если он есть
                next(reader, None)
                #Читаем данные из файла и вставляем их в таблицу customers
                for row in reader:
                    cur.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)', row)

    #Коммитим транзакцию
    conn.commit()
    print('Данные успешно вставлены в БД')
except (Exception, psycopg2.Error) as error:
    print('Ошибка при вставке данных')