from datetime import datetime
import pandas as pd
import string
import re

from mysql_db import table_management
from config import config_2

date = datetime.now()


#zmienne do logowania do bazy
hostname = config_2['hostname_ovh']
dbname = config_2['dbname_ovh']
uname = config_2['uname_ovh']
pwd = config_2['pwd_ovh']

table_config_table = config_2['table_config_table']
table_aalerts_backend = config_2['table_aalerts_backend']
table_rejestr_io_subsidiaries = config_2['table_rejestr_io_subsidiaries']
table_rejestr_io_company_info = config_2['table_rejestr_io_company_info']


"""
Do napisania pozostał algorytm filtrujący spółki - z ręcznei ustawianymi opcjami punct, lower, usuwania formy działalności
Wtedy będziemy mogli spokojnie odfitrować spółki typu W.P.C. od WPC Poland - obecnie w bazie jest recznie odrzucona spółki WPC Poland >
bo poland nigdy nie będzie bo poland jest wykasowywane z nazw spółek
"""

polish_words = {'ś': 's', 'ć': 'c', 'ó': 'o', 'ż': 'z', 'ź': 'z', 'ę': 'e', 'ł': 'l', 'ń': 'n', 'ą': 'a'}

checklst = ["spolka akcyjna", "sa", "s a", "plc", "as", "a s", "ltd", "nv", "se", "ag", "ad", "asi",
            "alternatywa spolka", "alternatywna spolka", "towarzystwo funduszy", "tfi"]

remove_after = ['spz oo', 'sp z oo', 'sp z o o', 's p z o o', 'sp zo o', 'sp zoo', 'spolki z ograniczona', 'sp z oo',
                'spolka z ograniczona', 'spolka z ograaniczona', 'spolka ograniczona', 'spzoo', 'spolka z oo', 'spolka zoo',
                'spolka z oo', 'sa', 's a', 'spolka akcyjna', 'spolkaakcyjna', 'sp k', 'spk', 'sp komandytowa',
                'spolka komandytowa', 'spolka jawna', 'spj', 'sp j']

replace_lst = checklst + remove_after
replace_lst = [" " + exchange_rates_dict + " " for exchange_rates_dict in replace_lst]
pattern_clean = "(" + '|'.join(replace_lst) + ")"

remove_all_string = ['sc', 's c', 'spolka cywilna', 'sca']  #Duet S.C.A.
remove_all_string = [" " + exchange_rates_dict + " " for exchange_rates_dict in remove_all_string]
pattern_remove = "(" + '|'.join(remove_all_string) + ")"


def alerts_table(info):
    """ system alertowania w przypadku awarii pobierania szczegółowych danych o jakiejś spółce """
    cls = table_management(hostname, dbname, uname, pwd)
    cls.add_data_row(table_aalerts_backend, [info, date, 'jobs_archive_pracuj.pl'], '(info,updated,table_name)', '(%s, %s, %s)')
    cls.close_connection_2()

def check_current_month():
    """ sprawdzanie czy w tabeli job_archive_dates_pracuj_pl jest obecny miesiąc """
    month = datetime.now().month
    year = datetime.now().year

    cls = table_management(hostname, dbname, uname, pwd)
    index_table = cls.fetch_one_result_filtered('job_archive_dates_pracuj_pl', 'id',
                                                f'year = {year} AND month = {month} AND is_finished IN (0,1)')

    if index_table is None:
        print("Brak obecnego miesiąca w bazie, aktualizacja...")
        dictionary = {"year": year,
                      "month": month,
                      "next_page_parse": 0,
                      "is_finished": 0,
                      "date_updated": datetime.now()}
        col_names_string = "(" + ",".join([str(i) for i in dictionary.keys()]) + ")"
        values_string = "(" + ", ".join(["%s"] * len(dictionary.keys())) + ")"
        data = list(dictionary.values())
        cls.add_data_row('job_archive_dates_pracuj_pl', data, col_names_string, values_string)
    cls.close_connection_2()

    months = [month]
    return months

def subsidiary_name_exceptions():  # domyślnie check_all = 1
    """ string które muszą wyjątkowo pasować 1:1 > tak jak BUDIMEX SPÓŁKA AKCYJNA SYGNITY """
    cls = table_management(hostname, dbname, uname, pwd)
    company_exceptions = cls.fetch_all_results_filtered('job_archive_companies_exceptions', 'company_keyword,subsidiary_name', f'to_check = 1')
    cls.close_connection_2()

    company_exceptions_dict = {}

    for elem in company_exceptions:
        search_string = edit_companies_name(elem[0])
        key = edit_companies_name(elem[1])

        if key in company_exceptions_dict.keys():
            company_exceptions_dict[key] = company_exceptions_dict[key] + [search_string]
        else:
            company_exceptions_dict[key] = [search_string]

    return company_exceptions_dict

def companies_excluded():
    """ spółki do wykluczenia > tak jak BUDIMEX SPÓŁKA AKCYJNA SYGNITY """
    cls = table_management(hostname, dbname, uname, pwd)
    company_exceptions = cls.fetch_all_results_filtered('job_archive_companies_exceptions', 'comp_id,company_keyword', f'to_check = 2')
    cls.close_connection_2()
    company_excluded_dict = {}

    for elem in company_exceptions:
        company_keyword = elem[1].lower()
        comp_id = elem[0]

        if comp_id in company_excluded_dict.keys():
            company_excluded_dict[comp_id] = company_excluded_dict[comp_id] + [company_keyword]
        else:
            company_excluded_dict[comp_id] = [company_keyword]
    return company_excluded_dict

def remove_civil_companies(value, string_cleaned):
    if re.search(fr'{pattern_remove}', value):
        return 'random xavd startersome random string'
    else:
        return string_cleaned

def clean_search_name(value_raw, flag):  # usuwanie końcówek do kolumny companies_search
    value_raw = ' random xavd starter' + value_raw + ' '  #dodajemy taki UNIKALNY ciąg znaków aby uniemożliwić odcięcia słowa od początku stringu
    for k in polish_words:  # zamiana polskich znaków
        value_raw = value_raw.lower().replace(k, polish_words[k])

    value_raw = value_raw.translate(str.maketrans('', '', string.punctuation))
    value = re.split(pattern_clean, value_raw)  # oddzielamy po patternie
    string_cleaned = value[0].strip()
    if flag == 0:  #dla spółek z wniosków - usuwamy te spółki które są cywilne
        string_cleaned = remove_civil_companies(value_raw, string_cleaned)
    string_cleaned = string_cleaned.split('random xavd starter')[1]
    return string_cleaned.strip()

def edit_companies_name(string_raw, flag=0):  # jeśli flag == 2 to nie oczyszczamy stringu z SA i punct
    """ funkcja do oczyszczania nazw spółek """
    string_raw = string_raw.lower()
    string_raw = re.sub(r"\s+", " ", string_raw)
    string_cleaned = clean_search_name(string_raw, flag)
    string_cleaned = re.sub(r"\s+", " ", string_cleaned)
    return string_cleaned

def edit_subsidiary_name(string_object):
    string_object = re.sub(r"\s+", " ", string_object)
    return string_object.strip()


class company_dataframe:
    """ pobieranie i przygotowywanie listy spółek z BD """

    def __init__(self, check_all):
        self.check_all = check_all  #czy sprawdzamy spółki do sprawdzenia wszystkich wniosków czy tylko nowych

    def get_companies_df_main(self):
        """ pobieranie głównych spółek - w sumie nie potrzebujemy nic więcej niż kolumny w tabeli job_archive_companies_krs """
        cls = table_management(hostname, dbname, uname, pwd)
        frame = cls.fetch_all_results_filtered('job_archive_companies_krs', 'comp_id,name_search,krs', f'active=1 AND check_all = {self.check_all}')
        cls.close_connection_2()
        df = pd.DataFrame(frame, columns=['comp_id', 'company_name', 'company_krs'])
        df['if_subsidiary'] = 0  # bo nie są spółkami zależnymi
        df['subsidiary_name'] = df['company_name']
        df['subsidiary_krs'] = df['company_krs']  # tymczasowo żeby wyszukiwanie spółek działało lepiej
        return df

    def get_companies_df_subsidiary(self):
        """ pobieranie zależnych spółek z bazy rejestr_io wraz z krs itp """
        cls = table_management(hostname, dbname, uname, pwd)
        table_1 = table_rejestr_io_subsidiaries
        table_2 = 'job_archive_companies_krs'
        table_3 = table_rejestr_io_company_info
        where_condition = f'{table_1}.krs_comp_id = {table_2}.krs AND {table_2}.krs IS NOT NULL AND {table_3}.krs = {table_1}.krs_comp_id AND {table_2}.active = 1 AND {table_2}.check_all = {self.check_all}'  # AND {news_companies_company}.new = 0'
        cols_1 = ['krs_comp_id', 'name_short', 'krs']
        cols_2 = ['comp_id']
        cols_3 = ['name_short']
        df = cls.fetch_data_three_tables(table_1, table_2, table_3, cols_1, cols_2, cols_3, where_condition)
        cls.close_connection_2()
        df['if_subsidiary'] = 1  # bo nie są spółkami zależnymi
        df.columns = ['company_krs', 'subsidiary_name', 'subsidiary_krs', 'comp_id', 'company_name', 'if_subsidiary']
        return df

    def get_additional_subsidiary(self):
        """ pobieranie dodatkowych spółek zależnych dodanych ręcznie - inna nazwa np jak PGE GIEK
            sprawdzamy po comp_id bo mogą być spółki bez numeru krs
        """
        cls = table_management(hostname, dbname, uname, pwd)
        table_1 = 'job_archive_companies_words'
        table_2 = 'job_archive_companies_krs'
        where_condition = f'{table_1}.comp_id = {table_2}.comp_id AND {table_1}.if_subsidiary = 1 AND {table_2}.active = 1 AND {table_2}.check_all = {self.check_all}'
        cols_1 = ['company_keyword', 'krs_company']
        cols_2 = ['comp_id', 'name_search', 'krs']
        df = cls.fetch_data_multi_tables(table_1, table_2, cols_1, cols_2, where_condition)
        cls.close_connection_2()
        df['if_subsidiary'] = 1  # bo nie są spółkami zależnymi
        df = df.rename(columns={'krs': 'company_krs', 'idCompany': 'comp_id', 'krs_company': 'subsidiary_krs',
                                'name_search': 'company_name', 'company_keyword': 'subsidiary_name'})
        return df

    def get_additional_main_comp(self):
        """ pobieranie dodatkowych spółek głównych dodanych ręcznie - inna nazwa np jak PGE GIEK """
        cls = table_management(hostname, dbname, uname, pwd)
        table_1 = 'job_archive_companies_words'
        table_2 = 'job_archive_companies_krs'
        where_condition = f'{table_1}.comp_id = {table_2}.comp_id AND {table_1}.if_subsidiary = 0 AND {table_2}.active = 1 AND {table_2}.check_all = {self.check_all}'
        cols_1 = ['company_keyword', 'krs_company']
        cols_2 = ['comp_id', 'name_search', 'krs']
        df = cls.fetch_data_multi_tables(table_1, table_2, cols_1, cols_2, where_condition)
        cls.close_connection_2()
        df['if_subsidiary'] = 0  # bo nie są spółkami zależnymi
        df = df.rename(columns={'krs': 'company_krs', 'idCompany': 'comp_id', 'krs_company': 'subsidiary_krs',
                                'name_search': 'company_name', 'company_keyword': 'subsidiary_name'})
        return df

    def replace_words(self, df):
        replace_lst_1 = ['grupa kapitałowa', 'grupa kapitalowa']
        replace_lst_2 = ['grupa', 'group', 'holding', 'poland', 'polska']
        pattern_1 = "(" + '|'.join(replace_lst_1) + ")"
        pattern_2 = "(" + '|'.join(replace_lst_2) + ")"

        df['subsidiary_name'] = df['subsidiary_name'].replace(fr'(?i){pattern_1}', ' ', regex=True)
        df['subsidiary_name'] = df['subsidiary_name'].replace(fr'(?i){pattern_2}', ' ', regex=True)
        df['subsidiary_name'] = df['subsidiary_name'].replace(fr'\s+', ' ', regex=True)
        return df

    def remove_specific_subsidiaries(self, df):
        """ usuwanie spółek zależnych celowo na początku > takich jak BUDIMEX SPÓŁKA AKCYJNA SYGNITY """
        comp_excluded_dict = companies_excluded()

        index_drop_lst = []
        for comp_id, keywords_excluded in comp_excluded_dict.items():
            df_comp_id = df[df['comp_id'] == comp_id]
            if df_comp_id.empty is False:
                dict_index = dict(zip(df_comp_id.subsidiary_name.str.lower(), df_comp_id.index))
                if any(subsidiary_name in keywords_excluded for subsidiary_name in dict_index.keys()):
                    for subsidiary_name in dict_index.keys():
                        if subsidiary_name in keywords_excluded:
                            index_row = dict_index[subsidiary_name]
                            index_drop_lst.append(index_row)
        if len(index_drop_lst) > 0:
            df.drop(index_drop_lst, axis=0, inplace=True)
        return df

    def join_dataframes(self):
        """
        funkcja do łączenia dwóch dataframów i przygotowywania listy do wyszukiwania spółek
        :return: zwracany słownik plus dataframe
        """
        df_1 = self.get_companies_df_main()
        df_2 = self.get_companies_df_subsidiary()

        df_1_add = self.get_additional_main_comp()      #dodatkowe nazwy spółek matek - głównych
        df_2_add = self.get_additional_subsidiary()     #dodatkowe nazwy spółek zależnych

        df = pd.concat([df_1, df_2, df_1_add, df_2_add]).reset_index(drop=True)
        df2 = df.copy()
        df2 = self.replace_words(df2)
        df2['subsidiary_name'] = df2['subsidiary_name'].replace(r'[;/&".:\!?]', ' ', regex=True)
        compared = df2.where(df['subsidiary_name'] != df2['subsidiary_name']).dropna(subset=['subsidiary_name'])
        df_final = pd.concat([df, compared]).reset_index(drop=True)

        if df_final.empty is True:
            return None
        else:
            df_final['subsidiary_name'] = df_final.apply(lambda row: edit_subsidiary_name(row['subsidiary_name']), axis=1)
            df_final['cleaned_string'] = df_final.apply(lambda row: edit_companies_name(row['subsidiary_name'], flag=1), axis=1)
            df_final = self.remove_specific_subsidiaries(df=df_final)
            return df_final



""" funckje do aktualizowania tabeli z ofertami pracy ze spółek giełdowych """

def get_start_id():
    """ sprawdzanie ostatniego wnisku przeanalizowanego przez algorytm >> to od niego będzie się odpalała kolejna aktualizacja """
    cls = table_management(hostname, dbname, uname, pwd)
    startID = cls.fetch_one_result_filtered(table_config_table, 'config', 'id = 11')
    cls.close_connection_2()
    return startID[0]

def update_start_id(startID_next):
    """ Po każdym batchsize aktualizujemy wartość ostatniego id job """
    cls = table_management(hostname, dbname, uname, pwd)
    cls.update_value(table_config_table, 'config', f'{startID_next}', 'id', '11')
    cls.update_value(table_config_table, 'updated_at', f'{date}', 'id', '11')
    cls.close_connection_2()

def get_max_job_id():
    """ Id wniosku najwyższego w surowej tabeli - stopID """
    cls = table_management(hostname, dbname, uname, pwd)
    stopID = cls.get_max_value('job_archive_jobs_pracuj_pl', 'id')
    cls.close_connection_2()
    return stopID
