from datetime import datetime
import pandas as pd

from job_offers_archive.archive_additional_code import get_start_id, get_max_job_id, alerts_table
#from job_offers_archive. import clean_table
from job_offers_archive.archive_find_companies_2 import search_jobs
from mysql_db import table_management
from config import config_2
from job_offers_archive.archive_additional_code import company_dataframe
from job_offers_archive.archive_find_excluded_companies import clean_table


"""
Wyszukiwanie spółek giełdowych w ofertach pracy

"""



date = datetime.now()

# zmienne do logowania do bazy
hostname = config_2['hostname_ovh']
dbname = config_2['dbname_ovh']
uname = config_2['uname_ovh']
pwd = config_2['pwd_ovh']


table_rejestr_io_companies = config_2['table_rejestr_io_companies']
job_offers_batchsize = 10000


"""
W pierwszej kolejności aktualizujemy tabelę job_archive_companies_krs - wszystkie stare spółki mają check_all = 0, a 
wszystkie nowe check_all = 1. Dla nowych spółek sprawdzamy wszystkie oferty pracy ID, a dla starych tylko nowe oferty pracy.

W spółkach możemy sprawdzać wszystkie - nawet nieaktywne giełowe spółki - no bo nie ma to żadnej różnicy

W przypadku nowej spółki giełdowej na aktualizację musimy czekać max 2 tygodnie
"""


class update_table_companies:
    """ klasa do aktualizacji tabeli ze spółkami - razem z active i check_all """

    def get_active_companies(self):
        """ wszystkie aktywne spółki giełdowe + jeśli są w tabeli rejestr_io_companies to numer krs """
        cls = table_management(hostname, dbname, uname, pwd)
        table_1 = 'companies'
        table_3 = 'rejestr_io_companies'

        mysql_statement = f'''SELECT {table_1}.id,{table_3}.krs,{table_1}.name_search
        FROM {table_1}
        LEFT JOIN {table_3}
        ON {table_1}.id={table_3}.comp_id
        WHERE {table_1}.active = 1'''

        frame = cls.fetch_result_fully_customized(statement=mysql_statement)
        cls.close_connection_2()
        df = pd.DataFrame(frame, columns=['comp_id', 'krs', 'name_search'])
        return df

    def update_new_companies(self):
        """ aktualizowanie listy spółek dla ofert pracy """
        cls = table_management(hostname, dbname, uname, pwd)
        old_companies = cls.fetch_all_results('job_archive_companies_krs', 'comp_id')  # bierzemy wszystkie comp_id
        old_companies = [x[0] for x in old_companies]

        df_active = self.get_active_companies()
        df_active['active'] = 1

        new_companies = df_active[~df_active['comp_id'].isin(old_companies)].copy()  # tylko nowe spółki
        old_comp_update = df_active[df_active['comp_id'].isin(old_companies)].copy()  # tylko stare spółki

        new_companies['check_all'] = 1
        print(f"Liczba spółek do wyszukania wszystkich ofert pracy: {len(new_companies)}")
        print(f"Liczba spółek do wyszukania tylko nowych ofert pracy: {len(old_companies)}")

        cls.set_column_value('job_archive_companies_krs', 'active', 0)  #zamieniamy całą tabelę na active=0

        self.save_row_to_DB(cls, data=new_companies, old_data=old_companies)
        self.save_row_to_DB(cls, data=old_comp_update, old_data=old_companies)
        cls.close_connection_2()

    def save_row_to_DB(self, cls, data, old_data):
        """ tutaj zapisujemy w pętli kolejne wiersz """
        col_names = data.columns.tolist()
        col_names_string = "(" + ",".join([str(i) for i in col_names]) + ")"
        values_string = "(" + ", ".join(["%s"] * len(col_names)) + ")"

        for row in data.to_dict('records'):
            comp_id = row['comp_id']
            data = list(row.values())
            if comp_id in old_data:  # jeśli mamy już w bazie to tylko aktualizujemy dane
                cls.update_values('job_archive_companies_krs', col_names, data, 'comp_id', comp_id)
            else:
                cls.add_data_row('job_archive_companies_krs', data, col_names_string, values_string)

    @staticmethod
    def change_check_all():
        cls = table_management(hostname, dbname, uname, pwd)
        cls.set_column_value('job_archive_companies_krs', 'check_all', 0)  #zamiana check_all wszystkich starych krs na 0 - ale dajemy po zaktualizowaniu danej spółki
        cls.close_connection_2()


def run_job_archive_companies():
    aktualizacja = update_table_companies()
    aktualizacja.update_new_companies()


    startID = get_start_id()  #ostatnie id wniosku przeanalizowanego ostatnio przez algorytm
    stopID = get_max_job_id()  #końcowe - najwyższe id wniosku w surowej tabeli z wnioskami

    print(f"ID zaczynające: {startID}, ID kończące: {stopID}")

    check_deleted = clean_table()
    check_deleted.update_table()

    # try:
    #     check_deleted = clean_table()
    #     check_deleted.update_table()
    # except Exception as e:
    #     info = f"Błąd przy kasowaniu odrzuconych ofert pracy ze złą spółką z tabeli job_archive_companies_exceptions, error: {e}"
    #     alerts_table(info)

    """ sprawdzanie tylko nowych ofert pracy dla danych spółek """
    data = search_jobs(startID=startID, stopID=stopID, batchsize=job_offers_batchsize, check_all=0)
    data.chunk_batches()

    # try:
    #     print("sprawdzanie tylko nowych ofert pracy dla danych spółek... check_all=0")
    #     data = analyze_gunb(startID=startID, stopID=stopID, batchsize=ebudownictwo_batchsize, check_all=0)
    #     data.chunk_batches()
    # except Exception as e:
    #     info = f"Błąd przy wyszukiwaniu ofert pracy spółek giełdowych - cały skrypt job_archive_jobs_pracuj_pl_companies, error: {e}"
    #     alerts_table(info)


    """ sprawdzanie wszystkich ofert pracy dla danych spółek """
    data = search_jobs(startID=0, stopID=stopID, batchsize=job_offers_batchsize, check_all=1)
    data.chunk_batches()
    data.check_new_comps()  # dodatkowo sprawdzamy czy pojawiły się nowe spółki - alert do bazy

    aktualizacja.change_check_all()  # po zakończeniu wyszukiwania nowych wniosków zmieniamy check_all=0

    # try:
    #     print("Sprawdzanie spółek w we wszystkich ofertach pracy do zaktualizowania... check_all=1")
    #     data = analyze_gunb(startID=0, stopID=stopID, batchsize=ebudownictwo_batchsize, check_all=1)
    #     data.chunk_batches()
    #     data.check_new_comps()  #dodatkowo sprawdzamy czy pojawiły się nowe spółki - alert do bazy
    # except Exception as e:
    #     info = f"Błąd przy wyszukiwaniu nowych ofert pracy spółek giełdowych - cały skrypt job_archive_jobs_pracuj_pl_companies, error: {e}"
    #     alerts_table(info)



