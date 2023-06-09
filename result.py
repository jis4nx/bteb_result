import re
import PyPDF2
import os
import pymongo
from pymongo.errors import BulkWriteError
from concurrent.futures import ThreadPoolExecutor
import shutil
import time


class ResultScraper:

    def __init__(self, create_dir=False):
        self.PATTERN = re.compile(r'\d{6}\s\{(.*?)\}|\d{6}\s\(.*?\)')
        self.NAME_PATTERN = r"\(Md\.KapayetUllah\)"
        self.DATE_PATTERN = r'Date\s.+(\d{2}-(\d{2})-(\d{4}))'
        self.SEM_PATTERN = r'(\d\w{2})\s\Semester'
        self.db_name = "bteb_result"
        self.dir_paths = ['txt', 'filtered']
        self.create_dir = create_dir

    def __enter__(self):
        if self.create_dir:
            for dir in self.dir_paths:
                try:
                    os.mkdir(dir)
                except FileExistsError:
                    pass
        else:
            pass
        try:
            self.client = pymongo.MongoClient(
                "localhost", 27017, serverselectiontimeoutms=10000)
            self.db = self.client[self.db_name]
        except Exception as e:
            print("Failed to connect to MongoDB")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.create_dir:
            for dir in self.dir_paths:
                shutil.rmtree(dir)
        else:
            pass
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

    def get_result(self, txt):

        with open(txt, 'r') as txt:
            text = txt.read()
            matches = re.finditer(self.PATTERN, text)
            year = re.search(self.DATE_PATTERN, text).group(3)
            collection_name = f'res_{year}'
            collection = self.db[collection_name]
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
            try:
                collection.create_index('roll', unique=True)
                collection.insert_many(self.RESULT_LIST, ordered=False)
                print(f"[+]Done  {self.db.name} -> {collection.name}")
            except BulkWriteError as e:
                for error in e.details['writeErrors']:
                    if error['code'] == 11000:
                        print("Found Duplicate, Skipping...")


t1 = time.perf_counter()
with ResultScraper(create_dir=True) as rs:
    rs.convert_pdf_to_text(os.listdir('pdf')[:3])
    rs.sanitize_text_files(os.listdir('txt'))

    for file in os.scandir('filtered'):
        rs.get_result(os.path.join('filtered', file.name))
t2 = time.perf_counter()-t1
print(t2)
