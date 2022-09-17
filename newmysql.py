from csv2db.sqlconnector import SqlConnector
import mysql.connector
from mysql.connector import errorcode


class MySQL(SqlConnector):

    def __init__(self, config, password):
        SqlConnector.__init__(self, config, password)

    def connect_server(self):

        cnx = mysql.connector.connect(database=self.config['database'], user=self.config['user'], password=self.password, host=self.config['host'], port=self.config['port'])
        return cnx

    def disconnect_server(self):
        return self.connect_server().close()

    def getTables(self):
        pass

    def getColumns(self, table):
        sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{}';".format(table)
        cnx = self.connect_server()
        cursor = cnx.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        list_columns = []
        for word in result:
            col = word[0]
            list_columns.append(col)
        return list_columns

    def add_column(self, table, col):
        sql = "alter table {} add {} varchar(255)".format(table, col)
        return sql

    def create_or_verify_table(self, table, header):
        type_var_header = []
        header_tuple = tuple(header)
        header_query = ", ".join(header_tuple)
        n = len(header_tuple)
        for i in range(n):
            var = header_tuple[i] + " varchar(255)"
            type_var_header.append(var)

        var_header = ", ".join(type_var_header)
        sql = "CREATE TABLE if not exists {} ({});".format(table, var_header)
        return sql

    def get_indice_col_database(self, col, table):
        col_values = self.getColumns(table)
        list_cols = list(col_values)
        #unique_key = self.config['unique_key']
        #length_key = len(unique_key)
        #list_indice = []
        index = list_cols.index(col)
        return index

    def columns_lower(self, list_col):
        new_list = []
        nb_col = len(list_col)
        for i in range(nb_col):
            new_list.append(list_col[i].lower().strip())
        return new_list

    def key_exist_in_dic(self, key, dic):
        if key in dic:
            return True

    def verify_existance_key_db_file(self, table, header):
        existance = False
        list_keys = self.config['unique_key']
        list_keys_lower = self.columns_lower(list_keys)
        length_keys = len(list_keys)
        columns = self.getColumns(table)
        columns_lower = self.columns_lower(columns)
        counter = 0
        for i in range(length_keys):
            if list_keys_lower[i] in columns_lower and list_keys_lower[i] in header:
                counter += 1
        if counter == length_keys:
            existance = True
        return existance

    def list_col_to_update(self, list_col):
        new_list = []
        list_keys = self.unique_key()
        length_list_col = len(list_col)
        for i in range(length_list_col):
            if list_col[i] not in list_keys:
                new_list.append(list_col[i])

        return new_list

    #def column_equal_file(self, table, dic, index):
    def columns_to_update(self, all_columns, dic):
        list_col = []

        n = len(all_columns)
        for i in range(n):
            if all_columns[i] not in dic:
                list_col.append(all_columns[i])
        return list_col


    def get_dic_keys(self, dic):
        keys = list(dic)
        return keys

    def get_dic_values(self, dic):
        values = list(dic.values())
        return values

    def get_insert_columns(self, mylist):
        list_tuple = tuple(mylist)
        list_query = ", ".join(list_tuple)
        return list_query

    def get_insert_values(self, mylist):
        var_values = []
        length = len(mylist)
        for i in range(length):
            var_values.append('%s')
        var_values_tuple = tuple(var_values)
        myvar_values = ", ".join(var_values_tuple)
        return myvar_values

    def header_exist_in_db(self, header, db_cols):
        counter = 0
        exist = False
        for i in range(len(header)):
            if header[i] in db_cols:
                counter += 1
        if counter == len(header):
            exist = True
        return exist

    def new_function(self, table, row, dic):
        result = ""
        keys = self.unique_key()
        keys_lower = self.columns_lower(keys)

    def get_index_uniqueKey(self, line_db, table):
        list_index = []
        keys = self.unique_key()
        columns_db = self.getColumns(table)
        columns = self.columns_lower(columns_db)
        for key in keys:
            index = columns.index(key)
            list_index.append(index)
        return list_index

    def insert_or_update(self, table, dic, columns):
        result = ""
        keys = self.config['unique_key']
        listKeys = self.columns_lower(keys)
        length_key = len(listKeys)

        #columns_db = self.getColumns(table)
        #columns = self.columns_lower(columns_db)

        sql = "select * from {}".format(table)
        cnx = self.connect_server()
        cursor = cnx.cursor()
        cursor.execute(sql)
        dataframe = cursor.fetchall()
        length_dataframe = len(dataframe)
        counter_for_insert = 0
        for line in dataframe:
            list_index = []
            counter = 0
            for j in range(length_key):
                index = self.get_indice_col_database(listKeys[j], table)
                if line[index] == dic[listKeys[j]]:
                    counter += 1
                    list_index.append(index)
            if counter == length_key:
                result = "update"
            else:
                counter_for_insert += 1

        if counter_for_insert == len(dataframe):
            result = "insert"
        return result

    def insert_into_table(self, table, dic):
        cnx = self.connect_server()
        cursor = cnx.cursor()

        dic_keys = self.get_dic_keys(dic)
        dic_values = self.get_dic_values(dic)
        query_header = self.get_insert_columns(dic_keys)
        query_values = self.get_insert_values(dic_values)
        row_query = tuple(self.get_dic_values(dic))

        sql_insert = "insert into {} ({}) values ({});".format(table, query_header, query_values)
        #print("la requete insert est")
        #print(sql_insert)
        cursor.execute(sql_insert, row_query)
        cnx.commit()
        #print("line inserted")


    def update_table(self, table, dic):
        cnx = self.connect_server()
        cursor = cnx.cursor()
        keys = self.unique_key()
        columns_update = []
        values_update = []
        columns_where_condition = []
        values_where_condition = []

        req_update = []

        for item in dic:
            if item not in keys:
                columns_update.append(item)
                values_update.append(dic[item])
            else:
                columns_where_condition.append(item)
                values_where_condition.append(dic[item])

        for i in range(len(columns_where_condition)):
            requete = "{} = '{}'".format(columns_where_condition[i], values_where_condition[i])
            req_update.append(requete)

        if len(columns_where_condition) > 1:
            variable = ""
            for count in range(1, len(columns_where_condition)):
                val =" and {}".format(req_update[i])
                variable += val
            condition_update = req_update[0] + variable
        elif len(columns_where_condition) == 1:
            condition_update = req_update[0]
        else:
            print("impossible de faire un update")

        for i in range(len(columns_update)):
            sql_update = "update {}" \
                         " set {} = '{}'" \
                         " where {};".format(table,columns_update[i], dic[columns_update[i]], condition_update)
            #print("la requete est")
            #print(sql_update)
            cursor.execute(sql_update)
            cnx.commit()
            #print("line updated")

    def lines_from_csv_no_db(self, header, columns):
        columns_not_exist = []

        for i in range(len(header)):
            if header[i] not in columns:
                columns_not_exist.append(header[i])
        return columns_not_exist
























































