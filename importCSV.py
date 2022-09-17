import os
import sys
import csv
from csv2db.config import getConfig
from csv2db.newmysql import MySQL
from csv2db.csvreader import CsvReader


nom_fichier = "fichier.csv"

table = 'agile4ux'
col = 'sent'


def create_dictionnary(list_col, list_values):
    length_col = len(list_col)
    length_values = len(list_values)
    if length_col == length_values:
        dic = {}
        for i in range(length_col):
            dic[list_col[i]] = list_values[i]

        return dic
    else:
        print("Dictionnaire ne peut pas être créer")


def columns_lower(list_col):
    new_list = []
    nb_col = len(list_col)
    for i in range(nb_col):
        new_list.append(list_col[i].lower().strip())
    return new_list


def transform_list(newlist):
    list_tuple = tuple(newlist)
    list_join = ", ".join(list_tuple)
    return list_join


def insert_line_database(row, header_query):
    global rows
    ligne = tuple(row)
    sql = "insert into {} ({}) " \
          "values " \
          "{};".format(table, header_query, ligne)
    return sql


if __name__ == '__main__':
    try:
        config = getConfig()
        password = input("pass: ")
        server = MySQL(config, password)
        cnx = server.connect_server()
        cursor = cnx.cursor()
    except:
        print("Erreur! vérifier la config ou Erreur unconnue")
    else:
        try:
            file = open(nom_fichier, 'r')
            reader = csv.reader(file, delimiter=";")
            header = []
            header = next(reader)
            header_lower = columns_lower(header)
            rows = []
            for line in reader:
                rows.append(line)

            nb_rows = len(rows)


        except:
            print("erreur lors de l'ouverture du fichier")
        else:
            try:
                length_header = len(header)

                sql_table = server.create_or_verify_table(table, header)
                cursor.execute(sql_table)
                cnx.commit()
                print("created or verified")

            except:
                print("erreur lors de la creation de la table")
            else:

                columns_db = server.getColumns(table)
                columns_db_lower = columns_lower(columns_db)
                # verifier si toutes les colonnes du fichier existent dans la base:
                exist = server.header_exist_in_db(header_lower, columns_db_lower)
                if exist is True:
                   # verifier si la clé unique existe dans le fichier et la base
                    key_exist = server.verify_existance_key_db_file(table, header_lower)
                    if key_exist is True:

                        sql = "select * from {}".format(table)
                        cursor.execute(sql)
                        dataframe = cursor.fetchall()

                        line_counter = 0
                        for row in rows:
                            line_counter += 1
                            row_lower = columns_lower(row)
                            dic = create_dictionnary(header_lower, row_lower)

                            result = server.insert_or_update(table, dic, columns_db_lower)
                            if result == "update":
                                server.update_table(table, dic)
                                print("ligne {} du fichier updated".format(line_counter))
                            else:
                                server.insert_into_table(table, dic)
                                print("ligne {} du fichier insérée".format(line_counter))


                    #case some columns do not exist in database
                else:
                    key_exist = server.verify_existance_key_db_file(table, header_lower)
                    if key_exist is True:
                        columns_not_exist = server.lines_from_csv_no_db(header_lower, columns_db_lower)
                        choice = ""
                        str = ", ".join(columns_not_exist)
                        while choice != "oui" and choice != "non":
                            choice = input("Voulez vous insérer les colonnes suivantes: {} ?  ".format(str))
                            if choice != "oui" and choice != "non":
                                print("J'ai pas compris! Veuillez choisir seulement oui/non")

                        if choice == "oui":
                            for i in range(len(columns_not_exist)):
                                if len(columns_not_exist) == 1:
                                    query_add_column = server.add_column(table, columns_not_exist[0])

                                else:
                                    query_add_column = server.add_column(table, columns_not_exist[i])

                                cursor.execute(query_add_column)
                                cnx.commit()
                                print("colonnes ajoutées")
                                line_counter = 0
                                for row in rows:
                                    line_counter += 1
                                    row_lower = columns_lower(row)
                                    dic = create_dictionnary(header_lower, row_lower)

                                    result = server.insert_or_update(table, dic, columns_db_lower)
                                    # print(result)
                                    if result == "update":
                                        server.update_table(table, dic)
                                        print("ligne {} du fichier updated".format(line_counter))
                                        percentage = line_counter * 100 / nb_rows
                                        #print("Le pourcentage est: {} %".format(round(percentage)))
                                    else:
                                        server.insert_into_table(table, dic)
                                        print("ligne {} du fichier insérée".format(line_counter))
                                        percentage = line_counter * 100 / nb_rows
                                        #print("Le pourcentage est: {} %".format(round(percentage)))


                        else:
                            choice = ""
                            while choice != "oui" and choice != "non":
                                choice = input("Voulez vous quand même importer les données?  ")
                                if choice != "oui" and choice != "non":
                                   print("J'ai pas compris! Veuillez choisir seulement oui/non")


                            if choice == "oui":
                                col_exist_query = []
                                index_col = []
                                n= len(header_lower)
                                #crer une liste avec les colonnes qui existent et une autre avec les indices des valeurs
                                for i in range(n):
                                    if header_lower[i] in columns_db_lower:
                                        col_exist_query.append(header_lower[i])
                                        index_col.append(i)
                                #recupérer les valeurs des colonnes qui existent deja
                                counter = 0
                                for row in rows:
                                    counter += 1
                                    existing_values = []
                                    for index in index_col:
                                        existing_values.append(row[index])
                                    #print(existing_values)
                                    #crer un dictionnaire avec les colonnes existantes
                                    dic = create_dictionnary(col_exist_query, existing_values)
                                    #print(dic)
                                    result = server.insert_or_update(table, dic, columns_db_lower)
                                    #print(result)
                                    if result == "update":
                                        server.update_table(table, dic)
                                        print("line {} du fichier updated".format(counter))
                                        percentage = counter * 100 / nb_rows
                                        #print("Le pourcentage est: {} %".format(round(percentage)))

                                    else:
                                        server.insert_into_table(table, dic)
                                        print("line {} du fichier insérée".format(counter))
                                        percentage = counter * 100 / nb_rows
                                        #print("Le pourcentage est: {} %".format(round(percentage)))


                            else:
                                print("Aucun changement de données dans la table")
                    else:
                        print("La clé unique : {} , n'existe pas dans le fichier CSV et/ou dans la base de données".format(server.unique_key()))

                cursor.close()
                cnx.close()























