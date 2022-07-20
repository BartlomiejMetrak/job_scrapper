# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ArchiveJobsItem(scrapy.Item):

    date_published = scrapy.Field()
    job_title = scrapy.Field()
    job_company = scrapy.Field()
    job_locs = scrapy.Field()
    job_link = scrapy.Field()
    job_id = scrapy.Field()
    current_page = scrapy.Field()
    year_month_str = scrapy.Field()
    index_table = scrapy.Field()



    def set_all(self, value=0):  # domyślnie każda wartość jest zero
        for keys, _ in self.fields.items():
            self[keys] = value
