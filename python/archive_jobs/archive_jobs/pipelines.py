from datetime import datetime
from .mysql_db import table_management
from .settings import config



#zmienne do logowania do bazy
hostname = config['hostname_ovh']
dbname = config['dbname_ovh']
uname = config['uname_ovh']
pwd = config['pwd_ovh']


date_now = datetime.now()  # data zapisu kolejnej liczby

class ArchiveJobsPipeline:

    cls = table_management(hostname, dbname, uname, pwd)

    def process_item(self, item, spider):
        dict_my = {
            "job_title": item["job_title"],
            "job_company": item["job_company"],
            "job_locs": item["job_locs"],
            "job_link": item["job_link"],
            "job_id": item["job_id"],
            "date_published": item["date_published"],
            "date_updated": date_now,
            "year_month_str": item['year_month_str'],
            "page": item['current_page'],
        }
        self.add_item_row(dict_my)
        # self.update_next_page_value(page=item["current_page"], index_table=item["index_table"])
        return item

    def add_item_row(self, dict_row):
        self.cls.add_data_row_or_update(table_name="job_archive_jobs_pracuj_pl", dictionary=dict_row)
        # col_names = dict_row.keys()
        # col_names_string = "(" + ",".join([str(i) for i in col_names]) + ")"
        # values_string = "(" + ", ".join(["%s"] * len(col_names)) + ")"
        # data = list(dict_row.values())
        #
        # self.cls.add_data_row('job_archive_jobs_pracuj_pl', data, col_names_string, values_string)  # dodać index

    def close_connection(self):
        """ zamykanie połączenia z bazą na końcu """
        self.cls.close_connection_2()
