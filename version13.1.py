import pandas as pd
import pymysql
import psycopg2
import sys
import os
import csv
import mysql.connector
from sqlalchemy import create_engine
from psycopg2 import OperationalError
import fonction_mysql as f_mysql
import fonction_postgres as f_postgres

host_name = "127.0.0.1"
db_name = "mydatabase"
table_name = "dataset"
nom_fichier = 'fichierTry.csv'
port_postgres = 5432
port_mysql = 3306


def affichage_parametre_serveur():
    global choix
    print("Hôte: ", host_name)
    print("Base de données: ", db_name)
    print("Table: ", table_name)
    if choix == '1':
        print("Type de la base de données: MySQL")
        print("Port: ", port_mysql)
    elif choix == '2':
        print("Type de la base de données: PostgreSQL")
        print("Port: ", port_postgres)
    else:
        print("erreur, type de base de données non reconnu")


def choisir_serveur():
    global choix, user_name, password_user
    print("\n")
    if choix == '1':
        try:
            cnx = mysql.connector.connect(database=db_name, user=user_name, password=password_user, host=host_name)
            affichage_parametre_serveur()

        #except mysql.connector.Error as err:
           # print(err)
        except:
            print("utilisateur et/ou mot de passe incorrecte(s)")

    else:
        try:
            cnx = psycopg2.connect(database=db_name, user=user_name, password=password_user, host=host_name)
            affichage_parametre_serveur()

        except psycopg2.OperationalError as err:
            print(err)
        except:
            print("utilisateur et/ou mot de passe incorrecte(s)")
    return cnx


def db_columns_lower(list_col):
    new_list = []
    nb_col = len(list_col)
    for i in range(nb_col):
        new_list.append(list_col[i].lower().strip())
    return new_list


def pandas_columns():
    global choix
    sql_query = "select * from {}".format(table_name)
    if choix == '1':
        valeur_colonnes = f_mysql.mysql_connect_engine(user_name, password_user, host_name, port_mysql, db_name, sql_query)
    else:
        valeur_colonnes = f_postgres.postgres_connect_engine(user_name, password_user, host_name, port_postgres, db_name, sql_query)
    return valeur_colonnes


def insert_line_all_columns_exist(row, header_query):
    global rows
    ligne = tuple(row)
    var_values = []
    row_query = []
    for i in range(len(ligne)):
        var_values.append('%s')
    # print("la varible ligne = tuple est ", ligne)
    ligne_chaine = ", ".join(ligne)
    # print("ligne_chaine = ", ligne_chaine)

    # print('var values **********', var_values)
    variable_values = tuple(var_values)
    # print("variable values est ", variable_values)
    myvar_values = ", ".join(variable_values)
    # print("myvar est ", myvar_values)
    sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name, header_query, myvar_values)
    return sql


def transform_list(newlist):
    list_tuple = tuple(newlist)
    list_join = ", ".join(list_tuple)
    return list_join


if __name__ == '__main__':
    choix = '0'
    while choix != '1' and choix != '2':
        choix = input("Voulez vous utiliser 1: MySQL ou 2: PostgreSQL? ")
        if choix != '1' and choix != '2':
            print("j'ai pas compris! réessayez SVP et choisissez seulement 1 ou 2\n")

    user_name = input("Entrez le nom d'utilisateur: ")
    password_user = input("Entrez le mot de passe: ")
    cnx = choisir_serveur()
    cursor = cnx.cursor()
    try:
        file = open(nom_fichier, 'r')
        reader = csv.reader(file, delimiter=";")
        # create table if it doesn't exist, extracting the columns from the csv file
        type_var_header = []
        header = []
        header = next(reader)
        # print("les colonnes sont")
        # print(header)
        header_tuple = tuple(header)
        # print("header tuple! ", header_tuple[0])
        header_query = ", ".join(header_tuple)
        # print("head_query ", header_query)
        # print(header_query)
        n = len(header_tuple)
        # print("n = ", n)
        for i in range(n):
            var = header_tuple[i] + " varchar(255)"
            type_var_header.append(var)
        # print(type_var_header)
        var_head = ", ".join(type_var_header)
        # print(var_head)
        sql = "CREATE TABLE if not exists {} ({});".format(table_name, var_head)
        cursor.execute(sql)
        cnx.commit()
        print("Table vérifiée ou créée\n")

        #le reste du fichier
        rows = []
        for line in reader:
            rows.append(line)
        #print(rows)

        nb_rows = len(rows)
        #print("nb_rows ", nb_rows)
        row_counter = 0
    except:
        print("\nErreur lors de l'ouverture du fichier")

    try:
        valeur_colonnes = pandas_columns()
        #print("les colonnes de la base de données sont : \n", valeur_colonnes)
        columns_lower = db_columns_lower(valeur_colonnes)
        #print("columns lower")
        #print(columns_lower)

        #verifier l'existance des colonnes du fichier dans la base de données

        columns_not_exist = []
        for i in range(n):
            if header_tuple[i].lower() in columns_lower:
                print("la colonne {} existe déja".format(header_tuple[i]))
                continue
            else:
                #print("la colonne {} n'existe pas".format(header_tuple[i]))
                print("\n")
                columns_not_exist.append(header_tuple[i])
                continue

        print("les colonnes qui n'existent pas dans la base sont: ")
        print(columns_not_exist)

        #traiter les colonnes qui n'existent pas
        choice_add_columns = ""
        if len(columns_not_exist) != 0:
            #il y'a des colonnes qui n'existent pas dans la base
            while choice_add_columns.lower() != "oui" and choice_add_columns.lower() != "non":
                choice_add_columns = input("Êtes vous sûr(e) de vouloir ajouter les colonnes suivantes {} dans la table {}? répondez par 'oui' ou 'non' SVP! ".format(columns_not_exist, table_name))
                if choice_add_columns.lower() != "oui" and choice_add_columns.lower() != "non":
                    print("J'ai pas compris! Réessayez SVP et choisissez seulement oui/non\n")
            print("\n")

            nb_col_not_exist = len(columns_not_exist)
            #print("votre choix est ", choice_add_columns)

            if choice_add_columns.lower() == "oui":
                #l'utilisateur choisit d'ajouter les colonnes
                #oui = ajouter les colonnes
                for i in range(nb_col_not_exist):

                    query_add_col = f_mysql.add_column_mysql(table_name, columns_not_exist[i])
                    #print("la requete est: ")
                    #print(query_add_col)
                    cursor.execute(query_add_col)
                    cnx.commit()
                    #print("colonne ajoutée")
                print("les colonnes {} sont ajoutées avec succés à la table {}".format(columns_not_exist, table_name))
                print("*****************************************************************************")
                print("Import des données, merci de patienter")

                for row in rows:
                    row_counter += 1
                    #print("row: ", row)
                    sql_query = insert_line_all_columns_exist(row, header_query)
                    #print("la requete pour ajouter les ligne si on choisit oui est: ")
                    #print(sql_query)
                    cursor.execute(sql_query, tuple(row))
                    #print("ligne ajoutée avec succés")
                    cnx.commit()
                    print("La ligne {} insérée avec succés".format(row_counter))
                    percentage = row_counter * 100 / nb_rows
                    print("Le pourcentage est: " + str(round(percentage)) + "%")
                print("\r" + str(nb_rows) + "lignes importées")

            else:
                #l'utilisateur ne veut pas ajouter les colonnes
                choice_import_info = ""
                while choice_import_info.lower().strip() != "non" and choice_import_info.lower().strip() != "oui":
                    choice_import_info = input("Voulez-vous quand même importer les données sans ajouter les colonnes {} ?  ".format(columns_not_exist))
                    if choice_import_info.lower().strip() != "non" and choice_import_info.lower().strip() != "oui":
                        print("J'ai pas compris! Réessayez SVP et choisissez seulement oui/non")

                if choice_import_info.lower() == "non":
                    print("Pas d'insertion de ligne(s)!")

                else:
                    #print("ajouter les données quand même")
                    #extraire les colonnes  qui existent dans la base
                    col_exist_query = []
                    index_col = []
                    for i in range(n):
                        if header_tuple[i].lower() in columns_lower:
                            var = header_tuple[i]
                            col_exist_query.append(var)
                            index_col.append(i)
                            continue
                    #print("les colonnes qui existent deja sont: ")
                    #print(col_exist_query)
                    #print("les indices des colonnes")
                    #print(index_col)


                    for row in rows:
                        row_counter += 1
                        new_row = []
                        length = len(row)
                        for nb_item in range(length):
                            if nb_item in index_col:
                                new_row.append(row[nb_item])
                                continue

                        #print("new row = ", new_row)

                        list_columns_db = transform_list(col_exist_query)
                        #print("list_columns_db = ", list_columns_db)
                        list_item = transform_list(new_row)
                        sql_query = insert_line_all_columns_exist(new_row, list_columns_db)
                        #print("la requete d'insertion est: ")
                        #print(sql_query)

                        cursor.execute(sql_query, tuple(new_row))
                        cnx.commit()
                        print("La ligne {} insérée avec succés".format(row_counter))
                        percentage = row_counter * 100 / nb_rows
                        print("Le pourcentage est: " + str(round(percentage)) + "%")
                    print("\r" + str(nb_rows) + "lignes importées")
                        #print("done")

        else:
            #toutes les colonnes du fichier existent dans la base de données
            print("*****************************************************************************")
            print("Import des données, merci de patienter")
            print("les noms des colonnes sont: ")
            print(header_query)
            for row in rows:
                row_counter += 1
                sql_query = insert_line_all_columns_exist(row, header_query)
                #print("la requete sql_add_col est: ")
                #print(sql_query)
                cursor.execute(sql_query, tuple(row))
                print("La ligne {} insérée avec succés".format(row_counter))
                percentage = row_counter * 100 / nb_rows
                print("Le pourcentage est: " + str(round(percentage)) + "%")
                print("\n")
                cnx.commit()

            print("\r" + str(nb_rows) + "lignes importées")
            cursor.close()
            cnx.close()

    except:
        print("Erreur")

