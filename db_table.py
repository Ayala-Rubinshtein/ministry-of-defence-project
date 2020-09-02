from typing import Any, Dict, List
from file_operators import operators

import json
import db_api
import csv
import hashedindex


class DBTable(db_api.DBTable):
    index = {}
    num_of_line: int = 1
    index_primary_key: int = 0
    title = []

    def __init__(self, name, field, key_field_name):
        self.name = name
        self.fields = field
        self.key_field_name = key_field_name

    def count(self) -> int:
        return self.num_of_line - 1

    def insert_record(self, values: Dict[str, Any]) -> None:

        if values[self.key_field_name] in self.index.keys():
            raise ValueError

        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', mode='a', newline='') as db_file:
            db_writer = csv.writer(db_file)
            title = []

            for value in values.values():
                title.append(value)
            self.index[title[self.index_primary_key]] = self.num_of_line
            self.num_of_line += 1
            db_writer.writerow(title)

    def delete_record(self, key: Any) -> None:
        with open(f'{db_api.DB_ROOT}\\{self.name}.csv') as csv_file:
            csv_reader = csv.reader(csv_file)
            _list = []

            for row in csv_reader:
                if row[self.index_primary_key] != str(key):
                    _list.append(row)

        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', mode='w', newline="") as db_file:
            db_writer = csv.writer(db_file)

            for value in _list:
                db_writer.writerow(value)

        if len(_list) == self.num_of_line:
            raise ValueError
        self.num_of_line -= 1

    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)
            if len(criteria) == 2:
                clean_rows = [record for record in csv_reader if
                              not (operators(str(record[self.index_primary_key]), criteria[0].operator,
                                                            criteria[0].value) and operators(record[self.index_primary_key],
                                                                                        criteria[1].operator, criteria[1].value))]
            else:
                clean_rows = [record for record in csv_reader if
                              not operators(record[self.index_primary_key], criteria[0].operator, criteria[0].value)]

        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(self.title)
            csv_writer.writerows(clean_rows)
            self.num_of_line = len(clean_rows) + 1

    def get_record(self, key: Any) -> Dict[str, Any]:

        with open(f'{db_api.DB_ROOT}\\{self.name}.csv') as fd:
            reader = csv.reader(fd)
            param = [row for idx, row in enumerate(reader) if idx == self.index[key]]
        _dict = {}

        for i in range(len(param[0])):
            _dict[self.title[i]] = param[0][i]
        return _dict

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        """check if the item is exist"""
        with open(f'{db_api.DB_ROOT}\\{self.name}.csv') as csv_file:
            csv_reader = csv.reader(csv_file)
            _list = []

            for row in csv_reader:
                if row[self.index_primary_key] != str(key):
                    _list.append(row)
                else:
                    list_val = []

                    for i in range(len(self.title)):
                        if self.title[i] not in values.keys():
                            list_val.append(list(row)[i])
                        else:
                            list_val.append(values[self.title[i]])
                    _list.append(list_val)

        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', mode='w', newline="") as db_file:
            db_writer = csv.writer(db_file)
            for value in _list:
                db_writer.writerow(value)

    def index_to_col(self, name):
        return self.title.index(name)

    def query_table(self, criteria: List[db_api.SelectionCriteria]) \
            -> List[Dict[str, Any]]:

        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)
            if len(criteria) == 2:
                clean_rows = [record for record in csv_reader if
                              (operators(record[self.index_to_col(criteria[0].field_name)], criteria[0].operator,
                                              criteria[0].value) and operators(
                                  record[self.index_to_col(criteria[0].field_name)],
                                  criteria[1].operator, criteria[1].value))]
            else:
                clean_rows = [record for record in csv_reader if
                              operators(record[self.index_to_col(criteria[0].field_name)], criteria[0].operator,
                                             criteria[0].value)]
        list_val = []

        for row in clean_rows:
            tmp = {}

            for i in range(len(row)):
                tmp[self.title[i]] = row[i]
            list_val.append(tmp)
        return list_val

    def create_index(self, field_to_index: str) -> None:
        index = hashedindex.HashedIndex()
        with open(f'{db_api.DB_ROOT}\\{self.name}.csv', "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)

            for i, row in enumerate(csv_reader, 2):
                index.add_term_occurrence(row[self.index_to_col(field_to_index)], i)

        with open(f'{db_api.DB_ROOT}\\{self.name}_{field_to_index}_index.json', "w") as json_file:
            json.dump(index.items(), json_file)




