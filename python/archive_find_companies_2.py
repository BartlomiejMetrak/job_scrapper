from timeit import default_timer as timer
import pandas as pd
from datetime import datetime
import numpy as np

from config import config_2
from mysql_db import table_management
from job_offers_archive.archive_additional_code import (company_dataframe, edit_companies_name,
                                                        subsidiary_name_exceptions, update_start_id, alerts_table)


date = datetime.now()
pd.set_option('display.max_columns', None)

# zmienne do logowania do bazy
hostname = config_2['hostname_ovh']
dbname = config_2['dbname_ovh']
uname = config_2['uname_ovh']
pwd = config_2['pwd_ovh']

table_job_archive_jobs_pracuj_pl_companies = 'job_archive_jobs_pracuj_pl_companies'
table_job_archive_jobs_pracuj_pl = 'job_archive_jobs_pracuj_pl'
table_config_table = config_2['table_config_table']


"""
skrypt do wyszukiwania ofert pracy spółek giełdowych

proces działania skryptu:
bierzemy listę spółek i spółek zależnych giełdowych - z uwzględnieniem tego w tabelach job_archive
wyszukujemy wzmianki o tych spółkach i przerzucamy do oddzielnej tabeli

w oddzielnej tabeli będą tylko oferty pracy od spółek giełdowych
kluczem do filtrowania jest job_id

W tabelach rejestr_io należy wyszukiwać po numerze KRS bo nie zawsze spółka może być prawidlowo wyszukana na podstawie tabeli companies_info
Ale po numerze krs zawsze zostanie dobrze wyszukane - potem przy aktualizacji numeru krs dane zostaną zaciągnięte dla nowego numeru
"""


"""
Sprawdzić potem czy odpowiednio pobieramy listę spółek - aktywne na gpw i stock market - moze także z poza rynku do tej tabeli czy nie?
"""

company_exceptions_dict = subsidiary_name_exceptions()


def get_companies(batch_start, batch_stop):
    """
    funkcja do pobierania listy ofert pracy z surowej tabeli pracuj.pl archive
    dla zmniejszenia przeciążenia bazy będziemy przerabiać dane częściowo po rozmiarze x = batchsize
    """
    cls = table_management(hostname, dbname, uname, pwd)
    jobs_data = cls.get_multi_filtered_columns_df(table_job_archive_jobs_pracuj_pl, 'id,job_id,job_title,job_company,job_locs,job_link,date_published,page,year_month_str',
                                                  f'id > {batch_start} and id <= {batch_stop}')
    cls.close_connection_2()
    jobs_data = jobs_data.dropna(subset=['job_company'])
    return jobs_data


def check_string(string, investor_1, investor_2):
    """
    wyszukiwanie spółki w stringu
    :param string: spółka z bazy - subsidiary_name - oczyszczona nazwa
    :param investor_1: oczyszczona nazwa inwestora
    :param investor_2: cskd + oczyszczona nazwa inwestora (żeby złapać tylko początek wyrazu)
    :return:
    """
    if string in company_exceptions_dict:  #jeśli string jest w wyjątkach typu >> BUDIMEX SPÓŁKA AKCYJNA SYGNITY
        subsidiary_variants_lst = company_exceptions_dict[string]
        if any(subsidiary_variant == investor_1 for subsidiary_variant in subsidiary_variants_lst):
            return True
        else:
            return False
    elif len(string) > 7:
        if ("cskd " + string + " ") in investor_2:
            return True
        else:
            return False
    elif len(string) < 2:
        return False
    else:
        if string == investor_1:
            return True
        else:
            return False

def get_jobs_id_in_DB():
    cls = table_management(hostname, dbname, uname, pwd)
    id_jobs = cls.fetch_all_results(table_job_archive_jobs_pracuj_pl_companies, 'job_id,comp_id')
    cls.close_connection_2()
    return id_jobs


class search_jobs:

    hash_id_codes = get_jobs_id_in_DB()
    new_companies_jobs = 0  # jeśli =1 to znaczy że zostały dodane nowe wnioski dla nowych spółek
    new_companies_comp = []  # lista nowych spółek dla których zostały dodane wnioski

    old_companies_jobs = 0  # nowe wnioski dla starych spółek giełdowych
    old_companies_comp = []  # lista ze spółkami których pojawiły się nowe wnioski

    def __init__(self, startID, stopID, batchsize, check_all):
        self.startID = startID  #id startujące
        self.stopID = stopID    #id kończące - w bazie w tabeli ebudownictwo - wnioski do przeanalizowania
        self.batchsize = batchsize  #wielkość jednej paczki do analizy
        self.check_all = check_all  #czy sprawdzamy spółki we wszystkich wnioskach czy tylko w nowych

    def chunk_batches(self):
        """ podzielenie id do sprawdzenia wg zadanych kryteriów - batchsize """
        print(f"Sprawdzanie ofert pracy dla check_all={self.check_all}\n")
        data = company_dataframe(check_all=self.check_all)  # dataframe z nazwami spółek do wyszukiwania - zależne + główne + dodane ręcznie
        df = data.join_dataframes()

        if df is not None:
            batches = np.arange(self.startID, self.stopID, self.batchsize)
            batches = np.append(batches, self.stopID)
            length = len(batches) - 1

            for i in range(length):
                start = batches[i]
                stop = batches[i + 1]
                print(f"Sprawdzanie ofert pracy z przedziału ID: {start} - {stop}")
                self.find_companies(batch_start=start, batch_stop=stop, df=df)
                #break
        else:
            print("Brak spółek w tabeli job_archive_companies_krs do zaktualizowanie - prawdopodobnie check_all=0")

    def find_companies(self, batch_start, batch_stop, df):
        """ wyszukiwanie nazw spółek giełdowych lub ich zależnych w ofertach pracy """
        jobs_data = get_companies(batch_start=batch_start, batch_stop=batch_stop)
        cleaned_string_df = df['cleaned_string'].to_list()
        frame = []
        id_lst = []

        for job in jobs_data.to_dict("records"):
            investor = job['job_company']
            if len(investor) > 1:
                id_lst.append(investor)
            investor_1 = edit_companies_name(investor, flag=0)
            investor_2 = "cskd " + investor_1 + " "

            if any(("cskd " + company + " ") in investor_2 if len(company) > 5 else company == investor_1 for company in
                   cleaned_string_df):
                df_filtered = df.loc[df['cleaned_string'].apply(lambda x: check_string(x, investor_1, investor_2) is True)]
                if df_filtered.empty is False:
                    df_dict = df_filtered.to_dict('records')
                    for row in df_dict:
                        dict_final = {**row, **job}
                        frame.append(dict_final)

        df = pd.DataFrame(frame)
        df_final = df.drop_duplicates(subset=['job_id', 'company_krs'], keep='first')  #dla danego wniosku możemy przyporządkować tylko jedną spółkę matkę

        if df_final.empty is False:
            self.update_DB(df_final)
            update_start_id(batch_stop)
            if self.check_all == 0:
                print(f"Wykryto {len(df_final)} nowych ofert pracy dla spółek giełdowych (check_all=0).\n")
                self.old_companies_jobs = self.old_companies_jobs + len(df_final)
                self.old_companies_comp = list(set(self.old_companies_comp + df_final['company_name'].unique().tolist()))
            else:  #jeśli sprawdzamy nowe spółki
                self.new_companies_jobs = self.new_companies_jobs + len(df_final)  #to znaczy że znaleźliśmy wnioski dla nowej spółki
                self.new_companies_comp = list(set(self.new_companies_comp + df_final['company_name'].unique().tolist()))  #nowa lista ze spółkami nowymi
        else:
            update_start_id(batch_stop)
            print(batch_stop)
            if self.check_all == 0:  #tylko jeśli sprawdzamy nowe id wniosków - a nie całość
                print("Nie wykryto żadnych nowych wniosków o zabudowę dla spółek giełdowych.")

    def update_DB(self, df):
        """ Przy aktualizowaniu bazy wniosków zakładamy że:
                > do jednego wniosku jest przyporzadkowana jedna spółki i datego w tabeli job_archive_jobs_pracuj_pl_companies job_id jest unikalne!!
                > dane mogą się zmienić i są wtedy aktualizowane - jeśli id_wniosku już jest w bazie """
        cls = table_management(hostname, dbname, uname, pwd)
        df = df.astype(object).where(pd.notnull(df), None)
        df = df.rename(columns={'id': 'job_offer_id'})

        col_names = df.columns.tolist()
        col_names_string = "(" + ",".join([str(i) for i in df.columns.tolist()]) + ")"
        values_string = "(" + ", ".join(["%s"] * len(df.columns)) + ")"

        for row in df.to_dict("records"):
            job_id = row['job_id']
            comp_id = row['comp_id']
            tuple = (job_id, comp_id)  #może być wniosek który należy do spółki córki dwóch spółek giełdowych - wtedy dwa wiersze - jeśli aktualizujemy taki wniosek to tylko ten wiersz
            data = list(row.values())
            if tuple in self.hash_id_codes:
                cls.update_values_condition(table_job_archive_jobs_pracuj_pl_companies, col_names, data, f'job_id = "{job_id}" AND comp_id = {comp_id}')  #updatujemy po hash_code
            else:
                cls.add_data_row(table_job_archive_jobs_pracuj_pl_companies, data, col_names_string, values_string)
            cls.update_value(table_config_table, 'config', f'{row["job_offer_id"]}', 'id', '11')
        cls.close_connection_2()

    def check_new_comps(self):
        """ funkcja do sprawdzania czy pojawiły się nowe spółki w BD i jeśli tak to dajemy alert do bazy """
        if self.new_companies_jobs != 0:  #jeśli nowe wnioski dla nowych spółek
            info = f"Aktualizacja oferty pracuj.pl - nowe spółki, liczba ofert: {self.new_companies_jobs}, spółki: {self.new_companies_comp}"
            alerts_table(info)
        if self.old_companies_jobs != 0:  # jeśli nowe wnioski dla starych spółek
            info_2 = f"Aktualizacja oferty pracuj.pl - stare spółki, liczba ofert: {self.old_companies_jobs}, spółki: {self.old_companies_comp}"
            alerts_table(info_2)
