import csv
import os


def read_csv(file_name):
    file_path = os.path.join(os.path.dirname(__file__), '..', 'data', file_name)
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            yield row