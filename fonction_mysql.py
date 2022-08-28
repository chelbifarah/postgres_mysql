import pandas as pd
import pymysql
from sqlalchemy import create_engine

#table = "data"
def mysql_connect_engine(user, password, host, port, database, sql):
    mydb = create_engine('mysql+pymysql://' + user + ':' + password + '@' + host + ':' + str(port) + '/' + database, echo=False)
    #cnx = mydb.connect()

    d = pd.read_sql(sql, mydb)
    valeur_colonnes = d.columns.values
    #cnx.close()
    return valeur_colonnes


def add_column_mysql(table_name, column_name):
    query_add_column = "alter table " + table_name + " ADD " + column_name + " varchar(255)"
    return query_add_column

def add_row_mysql(table_name, header_query, var_query):
    sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name, header_query, var_query)
    return sql
