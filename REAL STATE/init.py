import os
import pandas as pd
from datetime import datetime
import numpy as np
import re
import mysql.connector
import time

# Configurar as informações de conexão com o banco de dados MySQL
config = {
    'user': 'root',
    'password': '1234',
    'host': 'localhost',
    'database': 'global_stock_indices_historical_data',
}

# Estabelecer a conexão com o banco de dados
conn = mysql.connector.connect(**config)

# Criar um objeto cursor para executar comandos SQL
cursor = conn.cursor()



# Comando SQL para criar a tabela
create_table_query = f"CREATE TABLE IF NOT EXISTS {'propriedade'} (object_id INT PRIMARY KEY, name VARCHAR(255), state_pin VARCHAR(255), legal_description TEXT, operty_address VARCHAR(255),certified_land_value INT,certified_improvement_value INT,certified_total_value INT,owner_1 VARCHAR(255),owner_2 VARCHAR(255),owner_street VARCHAR(255),owner_city VARCHAR(255),owner_state CHAR(5),owner_zip VARCHAR(20),grade VARCHAR(10),year_built DATETIME DEFAULT NULL,property_condition CHAR(5) DEFAULT NULL,property_class VARCHAR(255),nbhd VARCHAR(255),sold_price FLOAT,conveyance_date DATETIME,legal_acres FLOAT,special_circumstances VARCHAR(255),shape_area FLOAT)"

# Executar o comando SQL para criar a tabela
cursor.execute(create_table_query)

# Confirmar as alterações e fechar a conexão


class HandlerDF:
    def __init__(self):
        csv_path = os.path.join(os.path.dirname(__file__),"./data/sales.csv")
        self.df = pd.read_csv(csv_path)

    def df_clear_nan(self):
        handler.df.fillna(' ', inplace=True)
        handler.df.replace(' ', None, inplace=True)

    def column_clear_nan(self,column):
            self.df[column] = self.df[column].apply(lambda x: None if pd.isna(x) else x)


    def show_column(self, column_name):
        print(self.df[column_name])

    def replace_to_uppercase(self, column_name):
        self.df[column_name] = self.df[column_name].str.upper()

    def replace_numbers_to_none(self, column_name):
        self.df[column_name] = self.df[column_name].apply(self.__replace_string_just_numbers)

    def __replace_string_just_numbers(self, cell):
        try:
            if isinstance(float(cell), float):
                return None
        except:
            return cell
        
    def get_date_from_timestamp(self, column_name):
        self.df[column_name] = self.df[column_name].apply(self.__split_timestamp_get_date)

    def __split_timestamp_get_date(self, cell):
        try:
            if isinstance(str(cell), str):
                return pd.to_datetime(cell.split(' ')[0], format='%Y/%m/%d')
            else:
                return None
        except:
            return None
        
    def change_name_column(self, column_name_old, column_name_new):
        columns = self.df.head(0)
        if column_name_old in columns:
            self.df.rename(columns={column_name_old: column_name_new}, inplace=True)
        else:
            print(f"A coluna '{column_name_old}' não existe no DataFrame.")

    def change_columns_to_lowercase(self):
        columns = self.df.head(0)
        for column in columns:
            self.df.rename(columns={column: column.lower()}, inplace=True)

    def apply_to_datetime(self, column_name):
        self.df[column_name] = pd.to_datetime(self.df[column_name], format='%d %B %Y')

    def dropna_column(self, column):
        self.df = self.df.dropna(subset=[column])


handler = HandlerDF()
handler.df_clear_nan()

handler.dropna_column('OBJECTID')

handler.replace_numbers_to_none('condition')
handler.replace_to_uppercase('condition')
handler.change_name_column('condition', 'property_condition')

handler.get_date_from_timestamp('ConveyanceDate')

handler.apply_to_datetime('year_built')

handler.change_columns_to_lowercase()


# handler.show_column('conveyancedate')
# handler.show_column('condition')
# print(handler.df.isna().sum())

conn = mysql.connector.connect(**config)
cursor = conn.cursor()

cursor.execute('DELETE FROM propriedade')
conn.commit()


for _, row in handler.df.iterrows():
    insert_query = """
    INSERT INTO propriedade (object_id, name, state_pin, legal_description, property_address, certified_land_value, 
    certified_improvement_value, certified_total_value, owner_1, owner_2, owner_street, owner_city, owner_state, 
    owner_zip, grade, year_built, property_condition, property_class, nbhd, sold_price, conveyance_date, legal_acres, 
    special_circumstances, shape_area)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    print(row)
    cursor.execute(insert_query, tuple(row))


conn.commit()
conn.close()

    
    
    # object_id INT PRIMARY KEY,
    # name VARCHAR(255),
    # state_pin VARCHAR(255),
    # legal_description TEXT,
    # property_address VARCHAR(255),
    # certified_land_value INT,
    # certified_improvement_value INT,
    # certified_total_value INT,
    # owner_1 VARCHAR(255),
    # owner_2 VARCHAR(255),
    # owner_street VARCHAR(255),
    # owner_city VARCHAR(255),
    # owner_state CHAR(5),
    # owner_zip VARCHAR(20),
    # grade VARCHAR(10),
    # year_built DATETIME DEFAULT NULL,
    # condition CHAR(5) DEFAULT NULL,
    # property_class VARCHAR(255),
    # nbhd VARCHAR(255),
    # conveyance_date DATETIME,
    # legal_acres FLOAT,
    # special_circumstances VARCHAR(255),
    # shape_area FLOAT [;l\][]