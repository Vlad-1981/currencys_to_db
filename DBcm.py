# import mysql.connector as mysql_conn
# import psycopg2 as pg_sql
# import pymysql
# import pymysql.connections

# ---------------------------------------------------------------
# Подключение к MySQLdb драйвером PyMySQL
# -----------------------

class ConnectionError(Exception):           #   возбуждается если БД недоступна (выключена или драйвер не сможет подключиться к БД)
    pass

class CredentialsError(Exception):          #   возбуждается при попытке ввести неправильные login и password
    pass

class SQLError(Exception):
    pass

class UserDatabase:

    def __init__(self, config: dict) -> None:
        self.configuration = config


    def __enter__(self) -> 'cursor':
        try:

            print("successfully connected...")
            print('-'.center(150, '-'))
            self.connection = pymysql.connect(**self.configuration)
            self.cursor = self.connection.cursor()
            return self.cursor

        except pymysql.err.InterfaceError as err:
            raise ConnectionError(err)

        except pymysql.err.ProgrammingError as err:                         #   возбуждается при попытке ввести неправильные login и password
            raise CredentialsError(err)


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.commit()
        self.connection.cursor()
        self.connection.close()
        if exc_type is pymysql.err.ProgrammingError:
            raise SQLError(exc_val)
        elif exc_type:
            raise exc_type(exc_val)


# ---------------------------------------------------------------
# Подключение к PostgreSQL
# -----------------------

# class UserDatabase:
#
#     def __init__(self, config: dict) -> None:
#         self.configuration = config
#
#
#     def __enter__(self) -> 'cursor':
#
#         self.connection = pg_sql.connect(**self.configuration)
#         self.cursor = self.connection.cursor()
#         return self.cursor
#
#         # except Exception as ex:
#         #     print(f"Возникла ошибка: {ex}")
#
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.connection.commit()
#         self.connection.cursor()
#         self.connection.close()

# ---------------------------------------------------------------
# Подключение к MySQLdb драйвером mysql.connector
# -----------------------

# class UserDatabase:
#
#     def __init__(self, config: dict) -> None:
#         self.configuration = config
#
#
#     def __enter__(self) -> 'cursor':
#         # try:
#
#             # print("successfully connected...")
#             # print('-'.center(50, '-'))
#         self.connection = mysql_conn.connect(**self.configuration)
#         self.cursor = self.connection.cursor()
#         return self.cursor
#
#         # except Exception as ex:
#         #     print(f"Возникла ошибка: {ex}")
#
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.connection.commit()
#         self.connection.cursor()
#         self.connection.close()























