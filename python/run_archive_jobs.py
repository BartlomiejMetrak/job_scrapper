from timeit import default_timer as timer
from scrapy.crawler import CrawlerProcess
from job_offers_archive.archive_jobs.archive_jobs.spiders.archive_jobs import all_archive_jobs
from job_offers_archive.archive_find_companies import run_job_archive_companies
from job_offers_archive.archive_additional_code import check_current_month

from config import config


""" Algorytm do pobierania historycznych ofert pracy dla pracuj.pl i praca.pl """

#zmienne do logowania do bazy
hostname = config['hostname_ovh']
dbname = config['dbname_ovh']
uname = config['uname_ovh']
pwd = config['pwd_ovh']


concurrent_requests_domain = 4


process = CrawlerProcess(settings={
    "ITEM_PIPELINES": {'archive_jobs.archive_jobs.pipelines.ArchiveJobsPipeline': 300, },
    "ROBOTSTXT_OBEY": True,
    "USER_AGENT": 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36',
    "DOWNLOAD_DELAY": 0.5,
    # "LOG_ENABLED": False,  #nie wyświetlamy niczego ze scrapy
    # "CONCURRENT_REQUESTS": 30,
    "CONCURRENT_REQUESTS_PER_DOMAIN": concurrent_requests_domain,
    "LOG_LEVEL": 'INFO',  #wyświetlanie tylko informacji o spider bot, bez pobranych danych
})


"""
Schemat:
    1. jeśli algorytm wykryje nowy miesiąc to dodajemy wiersz do tabeli w bazie danych z tym miesiacem i sprawdzamy
    poprzedni miesiąc.
    2. po zaktualizowaniu ostatniego miesiąca - odznaczamy go w bazie jako zakończony i już nie aktualizujemy w przyszłości
    3. sprawdzamy nowy miesiąc...
"""


def run_scrapy_jobs(update):
    process.crawl(all_archive_jobs, months=update)
    process.start()  # the script will block here until the crawling is finished


start = timer()

months_to_update = check_current_month()
run_scrapy_jobs(update=months_to_update)
#run_job_archive_companies()

end = timer()
print("\ncałkowity proces aktualizowania danych trwał: %s " % round((end - start), 2))

"""
sprawdzić tę ofertę:
https://www.pracuj.pl/praca/senior-sales-asistant-m-w-d-warszawa,oferta,1001687758
job_id - 1001687758

**  na początku oferta była wystawiona 29 marca 2022 roku jednak po aktualizacji w kwietniu data jej publikacji zmieniła się
    na 11 kwietnia 2022 roku. Na stronie data jej ważności to dalej 28 kwietnia czyli 30 dni od pierwszej publikacji
    
    > sprawdzić czy po 28 kwietnia umowa się przedłuży, jeśli nie to ok
"""


"""
Liczba stron pracuj.pl wg. miesięcy:
    > marzec 2022       - 2428
    > luty 2022         - 1886
    > styczeń 2022      - 1985
    > grudzień 2021     - 1619
    > listopad 2021     - 1867
    > październik 2021  - 1954
    > wrzesień 2021     - 1723
    > sierpień 2021     - 1739
    > lipiec 2021       - 1710
    > czerwiec 2021     - 1690
    > maj 2021          - 1680
    > kwiecień 2021     - 1465
    > marzec 2021       - 1540
    > luty 2021         - 1281
    > styczeń 2021      - 1184
    > grudzień 2020     - 877
    > listopad 2020     - 976
    > październik 2020  - 1159
    > wrzesień 2020     - 1129
    > sierpień 2020     - 937
    > lipiec 2020       - 998
    > czerwiec 2020     - 829
    > maj 2020          - 852
    > kwiecień 2020     - 535
    > marzec 2020       - 667
    > luty 2020         - 890
    > styczeń 2020      - 1029
    > grudzień 2019     - 657
    > listopad 2019     - 826
    > październik 2019  - 987
    > wrzesień 2019     - 925
    > sierpień 2019     - 872
    > lipiec 2019       - 976
    > czerwiec 2019     - 857
    > maj 2019          - 1037
    > kwiecień 2019     - 970
    > marzec 2019       - 1000
    > luty 2019         - 939
    > styczeń 2019      - 1126
    > grudzień 2018     - 693
    > listopad 2018     - 922
    > październik 2018  - 1047
    > wrzesień 2018     - 1074
    > sierpień 2018     - 953
    > lipiec 2018       - 956
    > czerwiec 2018     - 999
    > maj 2018          - 935
    > kwiecień 2018     - 961
    > marzec 2018       - 983 
    > luty 2018         - 909
    > styczeń 2018      - 1054
"""
