from datetime import datetime

from mysql_db import table_management
from job_offers_archive.archive_additional_code import subsidiary_name_exceptions, edit_companies_name
from config import config_2

date = datetime.now()


# zmienne do logowania do bazy
hostname = config_2['hostname_ovh']
dbname = config_2['dbname_ovh']
uname = config_2['uname_ovh']
pwd = config_2['pwd_ovh']

table_config_table = config_2['table_config_table']

table_job_archive_companies_exceptions = 'job_archive_companies_exceptions'
table_job_archive_jobs_pracuj_pl_companies = 'job_archive_jobs_pracuj_pl_companies'


"""
Pobieranie wartości w tabeli config - jeśli 1 to pobieranie całej listy spółek z exceptions
a następnie ponowne procesowanie ofert pracy lub zgloszeń z tabeli job_archive_jobs_pracuj_pl_companies
"""


class clean_table:

    def get_config(self):
        cls = table_management(hostname, dbname, uname, pwd)
        to_update = cls.fetch_one_result_filtered(table_config_table, 'config', 'id = 12')
        cls.close_connection_2()
        return to_update[0]

    def get_jobs_to_check(self, cleaned_subsidiary_name_lst):
        cls = table_management(hostname, dbname, uname, pwd)
        comp_ids = cls.fetch_all_results_filtered(table_job_archive_companies_exceptions, 'comp_id', 'to_check = 1')

        comp_ids_lst = [str(x[0]) for x in comp_ids]
        if len(comp_ids_lst) > 0:
            comp_ids_tpl = '("' + '","'.join(comp_ids_lst) + '")'
            cleaned_subsidiary_name_tpl = '("' + '","'.join(cleaned_subsidiary_name_lst) + '")'
            jobs_lst = cls.fetch_all_results_filtered(table_job_archive_jobs_pracuj_pl_companies, 'id,job_company', f'comp_id IN {comp_ids_tpl} AND cleaned_string IN {cleaned_subsidiary_name_tpl}')  #pobieramy tylko oferty pracy podejrzane o zmiane
            cls.close_connection_2()
            return [x for x in jobs_lst]
        else:
            return None

    def filter_data(self, jobs_lst, subsidiary_lst):
        """ filtrowanie po ofertach i szukanie tych źle dopasowanych > zamieniamy spację na '' oraz strip().lower() > jeśli flag = 2"""
        ids_to_delete = []
        for job_offer in jobs_lst:
            investor = edit_companies_name(job_offer[1])
            if investor not in subsidiary_lst:
                ids_to_delete.append(job_offer[0])
        return ids_to_delete

    def update_table(self):
        """
        jeśli są jakieś nowe spółki w tabeli ebudownictwo_companies_except i wnioski które ich dotyczą to
        sprawdzamy czy ich nazwa ulega zmianie i jeśli tak to kasujemy te złe
        przykład: ACTION S.A. i ACTION POLAND LOGISTICS (zła spółka) .... >> zmiana na dosłowną wartość dla ACTION S.A.
        """
        if self.get_config() == 1:
            companies_dict = subsidiary_name_exceptions()  #{subsidiary_name: [company_keyword]}
            subsidiary_lst = list(companies_dict.keys())

            jobs_lst = self.get_jobs_to_check(cleaned_subsidiary_name_lst=subsidiary_lst)

            if jobs_lst is not None:
                subsidiary_names_all = [j for i in companies_dict.values() for j in i]
                ids_to_delete = self.filter_data(jobs_lst=jobs_lst, subsidiary_lst=subsidiary_names_all)
                print(f"Liczba ofert pracy do wyskasowania: {len(ids_to_delete)}, \noferty: {ids_to_delete}")
                if len(ids_to_delete) > 0:
                    self.delete_rows(delete_lst=ids_to_delete)

    def delete_rows(self, delete_lst):
        cls = table_management(hostname, dbname, uname, pwd)
        cls.delete_rows_condition(table_job_archive_jobs_pracuj_pl_companies, f'id IN {tuple(delete_lst)}')
        cls.update_value(table_config_table, 'updated_at', f'{date}', 'id', '12')
        cls.update_value(table_config_table, 'config', '0', 'id', '12')
        cls.close_connection_2()



