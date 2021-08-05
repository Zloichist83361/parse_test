import json
import requests
import csv
import sqlite3

from sqlite3 import Error


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def main():
    database = r'data_kontur.db'

    sql_create_data_kontur_table = """ CREATE TABLE data_kontur (
                                        name_company text NOT NULL,
                                        inn integer,
                                        ogrn integer,
                                        date_register text
                                       ); """

    conn = create_connection(database)

    if conn is not None:
        create_table(conn, sql_create_data_kontur_table)
    else:
        print("Error! Cannot create the database connection.")

    with open('name.csv', encoding='utf-8') as File:
        reader = csv.reader(File, delimiter=',')

        for row in reader:
            search = row[0]
            url = 'https://focus.kontur.ru/api/search?country=RU&query='
            response = requests.get(url + search)
            data = response.text
            parsed = json.loads(data)

            for data_company in parsed['data']:
                name_comp = data_company['rawName']
                date_reg = data_company['regDate']
                requisites = data_company['requisites']
                inn = requisites[0]['value']
                ogrn = requisites[1]['value']
                all_data = [(name_comp, inn, ogrn, date_reg)]

                conn.cursor().executemany("INSERT INTO data_kontur VALUES (?,?,?,?)", all_data)
                conn.commit()


if __name__ == '__main__':
    main()
