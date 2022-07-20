import scrapy
from scrapy import exceptions
import collections
from ..items import ArchiveJobsItem
from datetime import datetime
from ..mysql_db import table_management

from ..settings import config
from ..pipelines import ArchiveJobsPipeline
import time



#zmienne do logowania do bazy
hostname = config['hostname_ovh']
dbname = config['dbname_ovh']
uname = config['uname_ovh']
pwd = config['pwd_ovh']


table_job_offers_companies = config['table_job_offers_companies']  #liczba ofert pracy
table_companies_profiles_jobs = config['table_companies_profiles_jobs']  #linki do portali
table_companies = config['table_companies']
table_aalerts_backend = config['table_aalerts_backend']
concurrent_requests_domain = 4


# def divide_to_multiplier(pages_to_scrape):
#     """ rozdzielanie na listy względem concurrent requests """
#     pages_start = []
#     for i in range(0, concurrent_requests_domain):  # dla każdej wielokrotności
#         multiplier_lst = [x for x in pages_to_scrape if x % concurrent_requests_domain == i]  # dzielimy na n grup
#         min_value = min(multiplier_lst)  # bierzemy min
#         pages_start.append(min_value)
#     print(pages_start)
#     return pages_start

def delete_wrong_pages(page_list, year_month_str):
    """ kasowanie stron które mają mniej niż 50 spółek """
    page_list = [str(x) for x in page_list] if len(page_list) > 0 else []
    page_tpl = '("' + '","'.join(page_list) + '")'
    print(f"Strony do wykasowania: {len(page_list)}")
    if len(page_list) > 0:
        cls = table_management(hostname, dbname, uname, pwd)
        cls.delete_rows_condition('job_archive_jobs_pracuj_pl', f'year_month_str = "{year_month_str}" AND page IN {page_tpl}')
        cls.close_connection_2()

def check_correctly_scrapped_pages(scrapped_pages, year_month_str):
    scrapped_pages = [int(x[0]) for x in scrapped_pages]
    c = collections.Counter(scrapped_pages)

    pages_to_scrapped_correctly = []
    pages_to_be_deleted = []
    for page, counter in c.items():
        if counter != 50:
            pages_to_be_deleted.append(page)
        else:
            pages_to_scrapped_correctly.append(page)
    delete_wrong_pages(page_list=pages_to_be_deleted, year_month_str=year_month_str)  # usuwanie z tabeli w DB strony które zostały źle pobrane
    return pages_to_scrapped_correctly

def check_comp_added_today(year, month, end_page, year_month_str):  #sprawdzanie spółek, które już sa w bazie z pracuj.pl
    cls = table_management(hostname, dbname, uname, pwd)
    index_table = cls.fetch_one_result_filtered('job_archive_dates_pracuj_pl', 'id', f'year = {year} AND month = {month} AND is_finished IN (0,1)')
    all_pages_scrapped = cls.fetch_all_results_filtered('job_archive_jobs_pracuj_pl', 'page', f'year_month_str = "{year_month_str}"')
    cls.close_connection_2()  # jeśli else to po prostu zostawiamy starą tabelę

    all_pages_scrapped = check_correctly_scrapped_pages(scrapped_pages=all_pages_scrapped, year_month_str=year_month_str)  # wszystkie poprawnie pobrane strony w bazie

    pages_to_scrape = [*range(1, end_page+2)]  # o jedną więcej niż ostatnia strona > bo range bez ostatniego (wyłącznie)
    pages_to_scrape = [x for x in pages_to_scrape if x not in all_pages_scrapped]
    print(f"Liczba stron do pobrania: {len(pages_to_scrape)}")
    return index_table[0], sorted(pages_to_scrape)  # divide_to_multiplier(pages_to_scrape=pages_to_scrape)

def update_month_finished(index_table, page):
    """ aktualizowanie informacji o tym że dany miesiąc z pracuj.pl został całkowicie pobrany """
    cls = table_management(hostname, dbname, uname, pwd)
    cls.update_value('job_archive_dates_pracuj_pl', 'is_finished', 1, 'id', index_table)  # dodać index
    cls.update_value('job_archive_dates_pracuj_pl', 'next_page_parse', page, 'id', index_table)  # dodać index
    cls.close_connection_2()


# Create Spider class
class all_archive_jobs(scrapy.Spider):
    """
    Każdy portal ma własny id:
    pracuj.pl:  0
    praca.pl:   1
    aplikuj.pl: 2
    """

    # Name of spider
    name = 'archive_jobs'
    allowed_domains = ['archiwum.pracuj.pl']

    def __init__(self, months, **kwargs):
        self.months = months
        super().__init__(**kwargs)

    def start_requests(self):
        year = 2022
        month = 4  # teraz kwiecień!
        max_page = 5000

        print(self.months)

        year_month_str = str(year) + "_" + str(month)

        index_table, pages_to_start = check_comp_added_today(year=year, month=month, end_page=max_page, year_month_str=year_month_str)
        print(pages_to_start)
        # for i in pages_to_scrape:
        #     url_link = f"https://archiwum.pracuj.pl/archive/offers?Year={year}&Month={month}&PageNumber={i}"
        #     yield scrapy.Request(url=url_link, meta={'index_table': index_table, 'year_month_str': year_month_str, 'max_page': max_page, 'current_page': i, 'try_counter': 1}, callback=self.parse_archive_jobs, errback=self.errback_func)

        for i in pages_to_start:
            # print(f"Numer strony: {i}")
            meta_dict = {'index_table': index_table,
                         'year_month_str': year_month_str,
                         'max_page': max_page,
                         'current_page': i,
                         'multiplier': i,
                         'try_counter': 0}

            url_link = f"https://archiwum.pracuj.pl/archive/offers?Year={year}&Month={month}&PageNumber={i}"
            #print(url_link)

            yield scrapy.Request(url=url_link, meta=meta_dict, priority=-1,
                                 callback=self.parse_archive_jobs, errback=self.errback_func)


    def parse_archive_jobs(self, response):
        """ funkcja do scrapowania stron razem z paginacją """
        print(response.url)
        page_num = int(response.url.split('=')[-1])
        response.meta['current_page'] = page_num
        response.meta['try_counter'] = response.meta['try_counter'] + 1

        index_table = response.meta['index_table']
        # max_page = response.meta['max_page']
        year_month_str = response.meta['year_month_str']

        if_next_page = response.css('div.offers div.offers_nav a.offers_nav_next::attr(href)').extract_first()  # następna strona do scrapowania
        all_jobs = response.css('.offers_item')
        print(f"Liczba ofert: {len(all_jobs)}.")

        if all_jobs is not None and len(all_jobs) > 0:
            for job in all_jobs:
                items = ArchiveJobsItem()
                items.set_all()

                date_published = job.css(".offers_item_desc_date::text").extract_first()
                job_title = job.css(".offers_item_link_cnt_part:nth-child(1)::text").extract_first()
                job_company = job.css(".offers_item_link_cnt_part+ .offers_item_link_cnt_part::text").extract_first()
                job_locs = job.css(".offers_item_desc_loc::text").extract_first()
                job_link = job.css('a.offers_item_link::attr(href)').extract_first()
                job_id = job_link.split(',')[-1]

                items['date_published'] = date_published
                items['job_title'] = job_title
                items['job_company'] = job_company
                items['job_locs'] = job_locs
                items['job_link'] = job_link
                items['job_id'] = job_id
                items['index_table'] = index_table
                items['current_page'] = page_num
                items['year_month_str'] = year_month_str
                yield items
        else:
            if all_jobs is not None and len(all_jobs) == 0 and if_next_page is not None:
                print(f"Strona pobrana raz jeszcze: {page_num}, try_counter: {response.meta['try_counter']}")
                if response.meta['try_counter'] < 3:
                    yield scrapy.Request(url=response.url, meta=response.meta, callback=self.parse_archive_jobs,
                                         errback=self.errback_func, dont_filter=True)  #dont_filter dla tych samych stron
                else:
                    print(f"Wykonano zbyt dużo prób odpytań do tej samej strony: {response.meta['try_counter']}")

        # if if_next_page is not None:  # jeśli istnieje kolejna strona
        #     next_page = '='.join(response.url.split('=')[:-1]) + f"={str(page_num+concurrent_requests_domain)}"
        #     print(next_page)
        #     yield scrapy.Request(url=next_page, meta=response.meta, callback=self.parse_archive_jobs, errback=self.errback_func)  # dont_filter dla tych samych stron
        if if_next_page is None:  # jeśli nie ma kolejnej strony to updatujemy tabelę
            if response.meta['try_counter'] < 3:
                print(f"Brak wykrytej strony - próba {response.meta['try_counter']}...")
                yield scrapy.Request(url=response.url, meta=response.meta, callback=self.parse_archive_jobs,
                                     errback=self.errback_func, priority=10, dont_filter=True)
            else:
                info = f"Koniec ofert pracy za dany miesiąc, podjęte próby ponownego sprawdzenia: {response.meta['try_counter']}"
                print(info)
                update_month_finished(index_table=index_table, page=page_num)
                close = ArchiveJobsPipeline()
                close.close_connection()
                raise exceptions.CloseSpider(reason=info)

    def errback_func(self, failure):
        """ funkcja do zarządzania jakimkolwiek błędem z pobierania szczegółowych danych o spółce z bankier.pl """
        date = datetime.now()
        request = failure.request

        info = f'Błąd przy pobieraniu danych o liczbie ofert pracy z portalu dnia {date}, link: {request.url} >> sprawdzić czy inne linki pobrały się czy był też błąd.'
        print(info)
        #cls = table_management(hostname, dbname, uname, pwd)
        #cls.add_data_row(table_aalerts_backend, [info, date, 'job_offers_companies'], '(info,updated,table_name)', '(%s, %s, %s)')
        #cls.close_connection_2()
