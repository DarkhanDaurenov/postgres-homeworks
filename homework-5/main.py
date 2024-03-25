import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    # Подключение к базе данных PostgreSQL
    conn = psycopg2.connect(**params)

    # Установка автокоммита, чтобы CREATE DATABASE не выполнялся внутри транзакции
    conn.autocommit = True

    # Создание курсора для выполнения операций с базой данных
    cur = conn.cursor()

    # Выполнение запроса для удаления базы данных
    cur.execute(f"DROP DATABASE {db_name}")

    # Выполнение запроса для создания базы данных
    cur.execute(f"CREATE DATABASE {db_name}")

    # Закрытие соединения, курсора
    cur.close()
    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""

    # Открывает файл и считывает содержимое
    with open(script_file, 'r') as file:
        cur.execute(file.read())


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    # SQL-запрос для создания таблицы suppliers
    cur.execute('''
                CREATE TABLE suppliers (
                    id SERIAL PRIMARY KEY,
                    company_name VARCHAR(255),
                    contact VARCHAR(255),
                    address VARCHAR(255),
                    phone VARCHAR(20),
                    fax VARCHAR(20),
                    homepage VARCHAR(255),
                    products TEXT[]
                )
            ''')


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, 'r') as file:
        suppliers_data = json.load(file)

    return suppliers_data


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    # Цикл по каждому поставщику в списке
    for supplier in suppliers:
        # SQL-запрос для вставки данных поставщика в таблицу suppliers
        cur.execute('''
                    INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage, products)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                ''', (
                supplier.get('company_name', ''),
                supplier.get('contact', ''),
                supplier.get('address', ''),
                supplier.get('phone', ''),
                supplier.get('fax', ''),
                supplier.get('homepage', ''),
                supplier.get('products', [])
            ))


def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""

    # Добавляем столбец supplier_id в таблицу products
    cur.execute('''
                ALTER TABLE products 
                ADD COLUMN supplier_id INT
            ''')

    # Добавляем внешний ключ
    cur.execute('''
        ALTER TABLE products
        ADD CONSTRAINT fk_suppliers_id
        FOREIGN KEY (supplier_id) 
        REFERENCES suppliers(id)
        ON DELETE CASCADE''')


if __name__ == '__main__':
    main()
