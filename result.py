import re
import PyPDF2
import os
import pymongo
from pymongo.errors import DuplicateKeyError
from concurrent.futures import ThreadPoolExecutor
import shutil


class ResultScraper:

    def __init__(self):
        self.PATTERN = re.compile(r'\d{6}\s\{(.*?)\}|\d{6}\s\(.*?\)')
        self.NAME_PATTERN = r"\(Md\.KapayetUllah\)"
        self.DATE_PATTERN = r'Date\s.+(\d{2}-(\d{2})-(\d{4}))'
        self.SEM_PATTERN = r'(\d\w{2})\s\Semester'
        self.db_name = "bteb_result"

    def __enter__(self):
        try:
            self.client = pymongo.MongoClient(
                "localhost", 27017, serverselectiontimeoutms=10000)
            self.db = self.client[self.db_name]
        except Exception as e:
            print("Failed to connect to MongoDB")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.client.close()

    def convert_pdf_to_text(self, pdf_list):
        def extract_text(pdf):
            file_name = os.path.splitext(pdf)[0]
            with open(f'pdf/{pdf}', 'rb') as f,\
                    open(f'txt/{file_name}.txt', 'w') as output:
                pdfdoc = PyPDF2.PdfFileReader(f)
                text = "\n".join(pdfdoc.getPage(page).extract_text()
                                 for page in range(pdfdoc.numPages))
                output.write(text)

        with ThreadPoolExecutor() as executor:
            executor.map(extract_text, pdf_list)

    def sanitize_text_files(self, text_list):
        for txtfile in text_list:
            name = os.path.splitext(txtfile)[0]
            with open(f'txt/{txtfile}', 'r') as output:
                all_the_data = output.read().replace('\n', '')
                all_the_data = re.sub(r',\n', '', all_the_data)
            with open(f'filtered/{name}.filtered.txt', 'w') as filtered:
                filtered.write(all_the_data)

    def insert_data(self, data):
        try:
            collection_name = f'res_{data["year"]}'
            collection = self.db[collection_name]
            collection.create_index('roll', unique=True)
            collection.insert_one(data)
            print(f"[+]Done  {self.db.name} -> {collection.name}")
        except DuplicateKeyError as e:
            print("Error: Duplicate key")

    def get_result(self, txt):

        with open(txt, 'r') as txt:
            text = txt.read()
            matches = re.finditer(self.PATTERN, text)
            year = re.search(self.DATE_PATTERN, text).group(3)
            collection_name = f'res_{year}'
            collection = self.db[collection_name]
            collection.create_index('roll', unique=True)
            semester = re.search(self.SEM_PATTERN, text).group(
                0).split(' ')[0][0]

            self.RESULT_LIST = []
            for match in matches:
                roll = match.group(0).split(' ')[0]
                raw_res = match.group(0).split(' ')[1:]
                string_res = "".join(raw_res)
                res = re.sub(self.NAME_PATTERN, "", str(string_res))
                if res.split():
                    final_result = None
                    if res.startswith("{"):
                        final_result = re.findall(r'\d+\(.+?\)', res)
                    elif res.startswith('('):
                        final_result = re.sub(r'\(|\)', '', res)
                    data = {
                        "year": year,
                        "semester": semester,
                        "roll": roll,
                        "result": final_result,
                    }
                    self.RESULT_LIST.append(data)

            with ThreadPoolExecutor() as executor:
                executor.map(self.insert_data, self.RESULT_LIST)


dir_paths = ['txt', 'filtered']

with ResultScraper() as rs:
    # rs.convert_pdf_to_text(os.listdir('pdf')[:3])
    # rs.sanitize_text_files(os.listdir('txt'))
    for file in os.scandir('filtered'):
        rs.get_result(os.path.join('filtered', file.name))

for dir_path in dir_paths:
    for file in os.scandir(dir_path):
        os.remove(os.path.join(dir_path, file.name))
