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
table_name = "data"
nom_fichier = 'fichierTry.csv'
port_postgres = 5432
port_mysql = 3306

def affichage_parametre_serveur():
    global choix
    print("Hôte: ", host_name)
    print("Base de données: ", db_name)
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

            return cnx
            # cursor = cnx.cursor()
        except mysql.connector.Error as err:
            print(err)
        except:
            print("utilisateur et/ou mot de passe incorrecte(s)")

    elif choix == '2':
        try:
            cnx = psycopg2.connect(database=db_name, user=user_name, password=password_user, host=host_name)
            affichage_parametre_serveur()
            return cnx
            # cursor = cnx.cursor()
        except psycopg2.OperationalError as err:
            print(err)
        except:
            print("utilisateur et/ou mot de passe incorrecte(s)")


def verifier_existance_colonne(col, list_col):
    global cursor, choix
    if col.lower().strip() in list_col:
        print("la colonne {} existe deja".format(col))
        print("\n")
    else:
        print("La colonne {} n'existe pas".format(col))
        if choix == '1':
            #query = fonction_mysql(table_name, col)
            query = f_mysql.add_column_mysql(table_name, col)
            #print("la requete est: ", query)
            cursor.execute(query)
            print("Nouvelle colonne {} ajouté à la table {}".format(col, table_name))
            cnx.commit()
            print("\n")
        elif choix == '2':
            query = f_postgres.add_column_postgresql(table_name, col)
            cursor.execute(query)
            print("Nouvelle colonne {} ajouté à la table {}".format(col, table_name))
            cnx.commit()
            print("\n")
        else:
            print("Erreur! impossible d'ajouter la colonne")


if __name__ == '__main__':
    #repeter la demande du choix jusqu'a le choix est 1 ou 2
    choix = '0'
    while choix != '1' and choix != '2':
        choix = input("Voulez vous utiliser 1: MySQL ou 2: PostgreSQL? ")
        if choix != '1' and choix != '2':
            print("j'ai pas compris! réessayez SVP et choisissez seulement 1 ou 2\n")

    #print("la valeur de choix est: ", choix)
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
        #print("header tuple! ", header_tuple[0])
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

        #afficher les colonnes de la base
        sql_query = "select * from {}".format(table_name)

        if choix == '1':
            valeur_colonnes = f_mysql.mysql_connect_engine(user_name, password_user, host_name, port_mysql, db_name, sql_query)
            print("les colonnes sont: ")
            print(valeur_colonnes)
        elif choix == '2':
            valeur_colonnes = f_postgres.postgres_connect_engine(user_name, password_user, host_name, port_postgres, db_name, sql_query)
            print("les colonnes sont: ")
            print(valeur_colonnes)
        else:
            print("Unknown error")
        print("\n")

        #verifier si les colonnes du fichier existent dans la base! si non on les ajoute
        for i in range(n):
            verifier_existance_colonne(header_tuple[i], valeur_colonnes)

        print("*****************************************************************************")
        print("Import des données, merci de patienter")

        #manipuler le reste du fichier

        rows = []
        for line in reader:
            rows.append(line)
        # print(rows)

        nb_rows = len(rows)
        row_counter = 0
        for row in rows:
            row_counter += 1
            ligne = tuple(row)
            var_values = []
            row_query = []
            for i in range(len(ligne)):
                var_values.append('%s')
            #print("la varible ligne = tuple est ", ligne)
            ligne_chaine = ", ".join(ligne)
            #print("ligne_chaine = ", ligne_chaine)

            #print('var values **********', var_values)
            variable_values = tuple(var_values)
            #print("variable values est ", variable_values)
            myvar_values = ", ".join(variable_values)
            #print("myvar est ", myvar_values)
            sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name, header_query, myvar_values)
            #print("la requete est: ")
            #print(sql)
            cursor.execute(sql, tuple(row))
            print("la ligne {} est insérée".format(row_counter))
            percentage = row_counter * 100 / nb_rows
            print("Le pourcentage est: "+str(round(percentage))+"%")
            print("\n")

        print("\r" + str(nb_rows) + "lignes importées")

        cnx.commit()
        cursor.close()
        cnx.close()

    except mysql.connector.Error as err:
        print(err)
    except psycopg2.OperationalError as err:
        print(err)
    except:
        print("other error")
