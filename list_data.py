import urllib.request
from datetime import timedelta
import datetime
import xml.dom.minidom as minidom
from pprint import pprint
from DBcm import UserDatabase, ConnectionError, CredentialsError, SQLError
from dbconfig import dbconfig
import pandas as pd
import csv
from dbconfig import extra_options_db
from dbconfig import dbconfig_2


def get_list_date() -> list:
    """
        Возвращает список выбранных дат
    :return: list
    """

    enter_date = input("Введите 'CD', чтобы выбрать текущую дату;\n"
                       "введите 'RD', чтобы выбрать диапазон дат; \n"
                       "введите 'MC', чтобы выбрать конкретную дату: \n").upper()

    if enter_date == 'CD':

        # url = f"https://www.nbrb.by/services/xmlexrates.aspx?ondate={datetime.now().strftime('%m/%d/%Y')}"
        data = datetime.datetime.now().strftime('%m/%d/%Y')
        return (data,)

    elif enter_date == 'RD':

        first_date = input("Введите начальную дату в формате ден/мес/год \n")
        last_date = input("Введите конечную дату в формате ден/мес/год \n")

        def format_date(date_:str) -> datetime.date:
            """
            Функция получает текущую дату и приводит её к отформатированному виду: 'Год - месяц - день'
            :param date: веденная дата
            :return: возвращает строку с отформатированной датой
            """

            god_ = int(date_[-4:])
            # print(god_)
            mes_ = int(date_[3:5])
            # print(mes_)
            den_ = int(date_[:2])

            return datetime.date(god_, mes_, den_)


        def get_list_range(first_date, last_date):


            range_of_date = []

            first_date = format_date(first_date)
            last_date = format_date(last_date)

            duration = last_date - first_date
            # return duration.days

            for d in range(duration.days + 1):
                day = (first_date + timedelta(days=d)).strftime("%m/%d/%Y")
                range_of_date.append(day)

            return range_of_date

        return get_list_range(first_date, last_date)


    elif enter_date == 'MC':

        date = input("Введите дату в формате ден/мес/год \n")

        def choice_date(enter_date):
            god_ = enter_date[6:10]
            mes_ = enter_date[3:5]
            den_ = enter_date[:2]
            conc_date = f'{mes_}/{den_}/{god_}'
            return conc_date

        data = choice_date(date)
        print(data)
        return (data,)


get_data = get_list_date()


def get_total_currencys_dict(func) -> dict:
    """
        Возвращает словарь курсов валют
    :param func: принимает список дат из функции "get_list_date"
    :return: dict
    """

    total_currency_dict = {}

    for i, j in enumerate(func):

        url = f"https://www.nbrb.by/services/xmlexrates.aspx?ondate={j}"


        def get_url_data(xml_url):
            web_file = urllib.request.urlopen(xml_url)
            return web_file.read()


        def get_currencys_dict(xml_content):
            dom = minidom.parseString(xml_content)
            dom.normalize()

            elements = dom.getElementsByTagName('Currency')

            currency_dict = {}
            list_ = []

            for node in elements:
                for child in node.childNodes:
                    if child.nodeType == 1:

                        if child.tagName == 'NumCode':
                            if child.firstChild.nodeType == 3:
                                num_code = str(child.firstChild.data)
                                list_.append(num_code)

                        if child.tagName == 'CharCode':
                            if child.firstChild.nodeType == 3:
                                char_code = child.firstChild.data

                        if child.tagName == 'Scale':
                            if child.firstChild.nodeType == 3:
                                scale = int(child.firstChild.data)
                                list_.append(scale)

                        if child.tagName == 'Name':
                            if child.firstChild.nodeType == 3:
                                name = str(child.firstChild.data)
                                list_.append(name)

                        if child.tagName == 'Rate':
                            if child.firstChild.nodeType == 3:
                                rate = float(child.firstChild.data)
                                list_.append(rate)

                currency_dict[char_code] = list_[:]
                list_.clear()

            return currency_dict

        total_currency_dict[j] = get_currencys_dict(get_url_data(url))

    return total_currency_dict


total_cur_dict = get_total_currencys_dict(get_data)


def get_list_db(db_name, database, **kwargs):
    """
        1) Принимаем значение словаря - название нашей БД
        2) Если такой БД нет, принимаем значение создаваемой БД из словаря "extra_options_db" = {'new_database': 'currencys'}
        3) Подключение к MySQL через словарь:  dbconfig_2 с параметрами БЕЗ ссылки на БД
    :param db_name:     Словарь с ссылкой на БД, в которую записываем курсы валют: dbconfig['database']
    :param database:    "extra_options_db" = {'new_database': 'currencys'}
    :param kwargs:      dbconfig_2 с параметрами БЕЗ ссылки на БД
    :return:
    """

    try:
        with UserDatabase(dbconfig) as cursor:

            check_all_table_in_db = f"SHOW DATABASES"

            cursor.execute(check_all_table_in_db)

            DB = cursor.fetchall()

            list_db = [item for item in [db for db in DB]]

            if DB:

                if len(DB) == 1:

                    for db in DB:

                        for item in db:
                            if item == db_name:
                                print(f"There is one 'DB' [ {db[0]} ] in list:\t [ {db_name} ]: ")
                                return db[0]
                            else:
                                print(f"There is one 'db' in list:\t [ {db_name} ]")
                                print(f"'DB':\t [ {db[0]} ]")

                if len(DB) > 1:

                    print(f"There are many 'DB' in the list:")
                    print(f"'DB':\t {list_db}")

                    for db in DB:
                        for item in db:
                            if item == db_name:
                                print(f"'DB' [ {db[0]} ] is in the list.")
                                return db[0]
                        break
            else:

                print(f"You haven't 'db' in the list. Create it!")

    except ConnectionError as err:
        print(f"Is your database switched on? Error:, {err}")   #   возбуждается если БД не доступна
    except CredentialsError as err:                             #   возбуждается при попытке ввести неправильные login и password
        print(f"User-id/Password issues. Error:, {err}")
    except SQLError as err:
        print(f"Is your query correct? Error: {err}")
    except Exception as err:
        print(f"Something went wrong: {err}")
        return 'Error'

    finally:

        with UserDatabase(kwargs) as cursor:

            create_db_query = f"CREATE DATABASE IF NOT EXISTS {database};"

            cursor.execute(create_db_query)

# check_db = get_list_db(dbconfig['database'], extra_options_db['new_database'], **dbconfig_2)

def get_list_tables_from_db(db_name, table_name):

    try:
        with UserDatabase(dbconfig) as cursor:

            check_all_table_in_db = f"SHOW TABLES FROM {db_name}"
            cursor.execute(check_all_table_in_db)

            tables = cursor.fetchall()

            list_table = [item for item in [table for table in tables]]

            if tables:

                if len(tables) == 1:

                    for table in tables:

                        for item in table:
                            if item == table_name:
                                print(f"There is one table [ {table[0]} ] in DB:\t [ {db_name} ]: ")
                                return table_name
                            else:
                                print(f"There is one table in DB:\t [ {db_name} ]")
                                print(f"Table:\t [ {table[0]} ]")

                if len(tables) > 1:

                    print(f"There are many tables in DB:\t [ {db_name} ]")
                    print(f"Tables:\t {list_table}")

                    for table in tables:
                        for item in table:
                            if item == table_name:
                                print(f"{table[0]}")
                                return table_name
                        break
            else:

                print(f"No table in DB:\t [ {db_name} ]. Create it!")

                _SQL = """CREATE TABLE IF NOT EXISTS exchange_rates (id int auto_increment primary key,
                                            RECEIVE_DATE timestamp default current_timestamp,
                                            DATE_OF_CURRENCY date not null,
                                            YEAR VARCHAR(4) not null,
                                            MONTH VARCHAR(2) not null,
                                            DAY VARCHAR(2) not null,
                                            USD float not null,
                                            EUR float not null,
                                            GBP float not null,
                                            RUB float not null,
                                            PLN float not null,
                                            JPY float not null,
                                            UAH float not null,
                                            CNY float not null) ENGINE MyISAM"""

                cursor.execute(_SQL)

                print(f"TABLE -\t '{table_name}' in DATABASE ->\t '{db_name}' CREATED successfully")



    except ConnectionError as err:
        print(f"Is your database switched on? Error:, {err}")   #   возбуждается если БД не доступна
    except CredentialsError as err:                             #   возбуждается при попытке ввести неправильные login и password
        print(f"User-id/Password issues. Error:, {err}")
    except SQLError as err:
        print(f"Is your query correct? Error: {err}")
    except Exception as err:
        print(f"Something went wrong: {err}")
        return 'Error'

# check_table_in_db = get_list_tables_from_db(dbconfig['database'], extra_options_db['table_name'])


def write_current_rates_to_currencys_db(**kwargs):

    for k, v in kwargs.items():

        new_format_date = datetime.datetime.strptime(k, '%m/%d/%Y').strftime('%d/%m/%Y')

        def cur_date(date_):             #   "12/11/2018"

            # date_time = "12/11/2018" # Considering date is in dd/mm/yyyy format
            date_time = datetime.datetime.strptime(date_, "%d/%m/%Y")
            # print(date_time)          #   2018-11-12 00:00:00

            day = date_time.strftime("%d")
            # print("date: ", day)
            month = date_time.strftime("%m")
            # print("month:", month)
            year = date_time.strftime("%Y")
            # print("year:", year)

            new_format_date = date_time, year, month, day
            # print(new_format_date)

            return new_format_date
        date_mn_year = cur_date(new_format_date)

        try:
            with UserDatabase(dbconfig) as cursor:
            # with connection.cursor() as cursor:

                db_name = dbconfig['database']
                table_name = 'exchange_rates'

                new_date = date_mn_year[0].date()

                check_query = f"SELECT * FROM {table_name} WHERE DATE_OF_CURRENCY='{new_date}'"
                # check_query = f"SELECT * FROM {table_name} WHERE DATE_OF_CURRENCY='{date_mn_year[0].date()}'"
                # check_query = f"SELECT * FROM {table_name} WHERE ID=1"

                cursor.execute(check_query)

                rows = cursor.fetchall()

                if rows:

                    print(f"The data [ {new_date} ] which you want to add is already in the [ {table_name} ]")
                    print('-' * 150)
                    for row in rows:
                        print(row)

                else:

                    _SQL = f"insert into {table_name} (DATE_OF_CURRENCY, YEAR, MONTH, DAY, USD, EUR, GBP, RUB, PLN, JPY, UAH, CNY)" \
                           f"values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"


                    tuple_ = (
                                # date_mn_year[0], date_mn_year[1], date_mn_year[2], date_mn_year[3],
                                *date_mn_year,
                                v['USD'][3],
                                v['EUR'][3],
                                v['GBP'][3],
                                v['RUB'][3],
                                v['PLN'][3],
                                v['JPY'][3],
                                v['UAH'][3],
                                v['CNY'][3],
                    )
                    print(tuple_)

                    cursor.execute(_SQL, (tuple_))

                    print(f"VALUE in - '{table_name}' for DATABASE ->\t '{db_name}' successfully added")


        except ConnectionError as err:
            print(f"Is your database switched on? Error:, {err}")   #   возбуждается если БД не доступна
        except CredentialsError as err:                             #   возбуждается при попытке ввести неправильные login и password
            print(f"User-id/Password issues. Error:, {err}")
        except SQLError as err:
            print(f"Is your query correct? Error: {err}")
        except Exception as err:
            print(f"Something went wrong: {err}")
            return 'Error'


def write_current_rates_to_csv():

    try:

        with UserDatabase(dbconfig) as cursor:

            db_name = dbconfig['database']
            table_name = 'exchange_rates'



            _SQL = f"SELECT * FROM {table_name};"
            # _SQL = f"SELECT * FROM {table_name} WHERE id BETWEEN 1 AND 50;"
            # _SQL = f"SELECT * FROM {table_name} ORDER BY DATE_OF_CURRENCY;"
            # _SQL = f"SELECT * FROM {table_name} WHERE id BETWEEN 1 AND 50;"

            # select_all_rows = f"SELECT * FROM {table_name}"
            cursor.execute(_SQL)
            rows = cursor.fetchall()

            print(len(rows))


            headers = ['ID', 'RECEIVE_DATE', 'DATE_OF_CURRENCY', 'YEAR', 'MONTH', 'DAY', 'USD', 'EUR', 'GBP', 'RUB', 'PLN', 'JPY', 'UAH', 'CNY']

            dict_data = dict.fromkeys(headers, None)

        # --------------------------------------------
        # 1-Й ВАРИАНТ (РЕШЕНИЕ ЧЕРЕЗ СПИСОК)
        # --------------------------------------------
            # for row in rows:
            #     if row not in rows:
            #         print(f'The table_name -> "{table_name}" is empty')
            #     else:
            #
            #         with open('data.csv', 'w', newline='') as csvfile:
            #             w = csv.writer(csvfile, delimiter=';')
            #             w.writerow(headers)
            #             for row in rows:
            #                 w.writerow(row)
            #         print(dict_data)

        # --------------------------------------------
        # 2-Й ВАРИАНТ (РЕШЕНИЕ ЧЕРЕЗ СЛОВАРЬ)
        # --------------------------------------------

            for row in rows:

                if row not in rows:
                    print(f'The table_name -> "{table_name}" is empty')

                else:
                    list_date = [dict(zip(headers, row)) for row in rows]
                    # dict_data = dict(zip(headers, row))
                    # list_date.append(dict_data)
                    # list_date.append(dict(zip(headers, row)))

            # with open('data_dict.csv', 'w', newline='', encoding='utf-8') as csvfile:
            #     writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=headers)
            #     writer.writeheader()
            #     writer.writerows(list_date)

        # --------------------------------------------
        # 3-Й ВАРИАНТ (РЕШЕНИЕ ЧЕРЕЗ PANDAS)
        # --------------------------------------------

            # list_data = []
            # for i in headers:
            #     for i in rows:
            #         list_data.append(i)

            # dict_ = dict(zip(headers, list_data))
            # with open('data_dict.csv', 'w', newline='', encoding='utf-8') as csvfile:
            df = pd.DataFrame(list_date)
            df_csv = df.to_csv(f"dict_usd.csv", index=False, sep=';', encoding='utf-8')

            #   -------->   ВСЕ ПОСЛЕДУЮЩИЕ МАНИПУЛЯЦИИ С PANDAS ОСУЩЕСТВЛЯЮТСЯ НА ОСНОВЕ СОРТИРОВКИ ПОЛЕЙ МЕТОДОМ SELECT   <-----------

            # df_csv = df[:10].to_csv(f"dict_usd.csv", index=False)   #   сохраняет в файл выборочные значения из БД
            # print(df.info)                            #   Получение сведений о датафрейме
            # print(df.describe())                      #   Вывод статистических сведений о датафрейме
            # print(df.USD.value_counts())              #   Подсчёт количества значений для конкретного столбца
            # print(df.columns.tolist())                #   Получение списка значений столбцов
            # print(df[['DATE_OF_CURRENCY', 'USD']])    #   Получение данных по конкретным столбцам
            # print(df.head(5))                         #   Получение данных начиная с головы БД
            # print(df.tail(5))                         #   Получение данных начиная с хвоста БД
            # print(df.iloc[0:3])                       #   Получение среза из БД
            # print(len(df))                            #   Подсчет длины датафрейма (если не было фильтра по SELECT)
            # print(df['USD'].tolist())                 #   Формирование списка данных из конкретного столбца
                                                            # >>> [2.5481, 2.5741, 2.9432, 2.5986, 2.5765, 2.5958, ...... ]
            # print(df.groupby('USD').count())            #   Надо смотреть в контексте других данных

                    # >>>             ID  RECEIVE_DATE  DATE_OF_CURRENCY  YEAR  ...  PLN  JPY  UAH  CNY
                    #         USD                                               ...
                    #         2.5315   1             1                 1     1  ...    1    1    1    1
                    #         2.5481   5             5                 5     5  ...    5    5    5    5
                    #         2.5555   5             5                 5     5  ...    5    5    5    5
                    #         2.5590   1             1                 1     1  ...    1    1    1    1
                    #         2.5613   2             2                 2     2  ...    2    2    2    2
                    #         2.5678   2             2                 2     2  ...    2    2    2    2
                    #         2.5689   2             2                 2     2  ...    2    2    2    2
                    #         2.5734   3             3                 3     3  ...    3    3    3    3
                    #         2.5741   2             2                 2     2  ...    2    2    2    2
                    #         2.5765   1             1                 1     1  ...    1    1    1    1
                    #         2.5774   2             2                 2     2  ...    2    2    2    2
                    #         2.5828   1             1                 1     1  ...    1    1    1    1
                    #         2.5829   2             2                 2     2  ...    2    2    2    2
                    #         2.5876   1             1                 1     1  ...    1    1    1    1
                    #         2.5886   4             4                 4     4  ...    4    4    4    4
                    #         2.5931   1             1                 1     1  ...    1    1    1    1
                    #         2.5945   1             1                 1     1  ...    1    1    1    1
                    #         2.5958   2             2                 2     2  ...    2    2    2    2
                    #         2.5986   1             1                 1     1  ...    1    1    1    1
                    #         2.6010   2             2                 2     2  ...    2    2    2    2
                    #         2.6035   1             1                 1     1  ...    1    1    1    1
                    #         2.6040   1             1                 1     1  ...    1    1    1    1
                    #         2.6109   1             1                 1     1  ...    1    1    1    1
                    #         2.6170   5             5                 5     5  ...    5    5    5    5
                    #         2.9432   1             1                 1     1  ...    1    1    1    1
                    #
                    #         [25 rows x 13 columns]

            # -----------------------------------------------------------------------------------------------------------

    except ConnectionError as err:
        print(f"Is your database switched on? Error:, {err}")   #   возбуждается если БД не доступна
    except CredentialsError as err:                             #   возбуждается при попытке ввести неправильные login и password
        print(f"User-id/Password issues. Error:, {err}")
    except SQLError as err:
        print(f"Is your query correct? Error: {err}")
    except Exception as err:
        print(f"Something went wrong: {err}")
        return 'Error'


def print_dict(dict):
    pprint(dict)
    # for key, value in dict.items():
    #     print(key, value)


# ----------------------------------
# DROP TABLE
# -----------------------------------
def del_table_from_db(kwargs):
    try:

        with UserDatabase(kwargs) as cursor:

            db_name = dbconfig['database']
            table_name = 'exchange_rates'

            drop_table_query = f"DROP TABLE {table_name};"
            cursor.execute(drop_table_query)

            print(f"TABLE -> '{table_name}' from DATABASE -> '{db_name}' delete successfully")

    except ConnectionError as err:
        print(f"Is your database switched on? Error:, {err}")   #   возбуждается если БД не доступна
    except CredentialsError as err:                             #   возбуждается при попытке ввести неправильные login и password
        print(f"User-id/Password issues. Error:, {err}")
    except SQLError as err:
        print(f"Is your query correct? Error: {err}")
    except Exception as err:
        print(f"Something went wrong: {err}")
        return 'Error'


# ----------------------------------
# DELETE DATA FROM TABLE
# -----------------------------------
def del_data_from_table(kwargs):
    try:

        with UserDatabase(kwargs) as cursor:

            db_name = dbconfig['database']
            table_name = 'exchange_rates'

            del_all_note_from_table = f"DELETE FROM {table_name};"
            cursor.execute(del_all_note_from_table)

            print(f"all notes from - '{table_name}' for DATABASE ->\t '{db_name}' successfully deleted")

    except ConnectionError as err:
        print(f"Is your database switched on? Error:, {err}")   #   возбуждается если БД не доступна
    except CredentialsError as err:                             #   возбуждается при попытке ввести неправильные login и password
        print(f"User-id/Password issues. Error:, {err}")
    except SQLError as err:
        print(f"Is your query correct? Error: {err}")
    except Exception as err:
        print(f"Something went wrong: {err}")
        return 'Error'


def main():
    # print_dict(total_cur_dict)
    get_list_db(dbconfig['database'], extra_options_db['new_database'], **dbconfig_2)
    get_list_tables_from_db(dbconfig['database'], extra_options_db['table_name'])
    write_current_rates_to_currencys_db(**total_cur_dict)
    write_current_rates_to_csv()


    # del_table_from_db(dbconfig)
    # del_data_from_table(dbconfig)


if __name__ == '__main__':
    main()





