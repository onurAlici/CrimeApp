import os
import pandas
from catalog_functions import catog
import psycopg2
from sqlalchemy import create_engine
import geopandas

path = "Filepath"

file_list = os.listdir(path)

final_list = []


for i in file_list:
    file1 = i
    if file1.startswith("."):
        continue
    file_list22 = os.path.join(path,file1)
    file_list2 = os.listdir(file_list22)

    for j in file_list2:
        file2 = j
        file_path = os.path.join(file_list22, file2)
        final_list.append(file_path)





db_connection_url = " ";
engine = create_engine(db_connection_url)




print("başladı...")
for k in final_list:
    forEveryFile(k)

print("bitti")
