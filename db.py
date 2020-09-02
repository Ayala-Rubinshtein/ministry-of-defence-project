from db_table import DBTable

from typing import Any, Dict, List
import db_api
import csv
import os
import json


class DataBase(db_api.DataBase):
    dict_table = {}

    def __init__(self):
        files = os.listdir(db_api.DB_ROOT)
        files = list(filter(lambda f: f.endswith('.csv'), files))
        for file in files:
            index = {}
            with open(f'{db_api.DB_ROOT}\\{file}', mode='r') as db_file:
                db_reader = csv.reader(db_file)
                for i, row in enumerate(db_reader, 1):
                    if i == 1:
                        title = row
                    index[row[0]] = i
                db: DBTable = DBTable(file[:-4], title, title[0])
            self.dict_table[file[:-4]] = db
            db.title = title
            db.index = index
            db.index_primary_key = 0
            db.num_of_line = i
            with open(f'{db_api.DB_ROOT}\\{file[:-4]}.json', "w") as outfile:
                data = {"key_field_name": db.key_field_name, "index_primary_key": db.index_primary_key}
                json.dump(data, outfile)

    def create_table(self,
                     table_name: str,
                     fields: List[db_api.DBField],
                     key_field_name: str) -> DBTable:
        db_table: DBTable = DBTable(table_name, fields, key_field_name)
        self.dict_table[table_name] = db_table
        with open(f'{db_api.DB_ROOT}\\{table_name}.json', "w") as outfile:
            data = {"key_field_name": key_field_name, "index_primary_key": db_table.index_primary_key}
            json.dump(data, outfile)
        with open(f'{db_api.DB_ROOT}\\{table_name}.csv', mode='w', newline='') as db_file:
            db_writer = csv.writer(db_file)
            for i in fields:
                db_table.title.append(i.name)

            db_writer.writerow(db_table.title)
        return db_table

    def num_tables(self) -> int:
        return len(self.dict_table)

    def get_table(self, table_name: str) -> DBTable:
        return self.dict_table[table_name]

    def delete_table(self, table_name: str) -> None:
        os.remove(f'{db_api.DB_ROOT}\\{table_name}.csv')
        if table_name in self.dict_table:
            del self.dict_table[table_name]

    def get_tables_names(self) -> List[Any]:
        return list(self.dict_table.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[db_api.SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

