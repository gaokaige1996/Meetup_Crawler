import html5lib, requests
import mysql.connector
import random
import time
import re
from bs4 import BeautifulSoup
from datetime import datetime

# database info
username = 'root'
password = '123456'
host = 'localhost'
dbase = 'meetup'


class crawl_group_intro:

    def __init__(self):
        self.dbconn = mysql.connector.connect(user=username, password=password, host=host, database=dbase)
        self.cursor = self.dbconn.cursor()

    def get_group_info(self):

        select_sql = (
            "SELECT distinct(group_url), group_name, group_city FROM meetup.all_groups where group_city = 'Washington' or group_city = 'New York' or group_city = 'Boston' or group_city = 'Chicago' or group_city = 'San Francisco' order by group_city"
        )
        self.cursor.execute(select_sql)

        for cursor_data in self.cursor.fetchall():
            group_url = cursor_data[0]
            group_name = cursor_data[1]
            group_city = cursor_data[2]

            print(group_url)

            wb_data = requests.get(group_url)
            soup = BeautifulSoup(wb_data.text, 'html5lib')

            group_city = group_city

            try:
                group_founded_date = soup.select('#C_metabox > div > div > div.small.margin-bottom')[0].get_text().split('Founded')[1].strip()
                crawl_flag = 1
            except IndexError:
                crawl_flag = 0
                print('Wrong Page')

            if crawl_flag == 1:

                try:
                    group_desc = soup.select('.groupDesc')[0].get_text()
                except IndexError:
                    group_desc = ''

                group_tags = soup.select('.meta-topics-block')[0].get_text().replace('\n', '').replace('\t', '').strip()

                sponsors_info = []

                try:
                    group_sponsors = soup.select('.wrapNice.align--center')

                    for temp_sponsor in group_sponsors:
                        sponsor_name = temp_sponsor.select('.hoverLink')[0].get_text()
                        sponsor_link = temp_sponsor.select('.hoverLink')[0]['href']
                        sponsor_info = sponsor_name + " : " + sponsor_link
                        sponsors_info.append(sponsor_info)

                except IndexError:
                    pass

                print(group_url, group_name, group_founded_date)

                args = (group_url, group_name, group_founded_date, group_desc, str(sponsors_info), group_city, group_tags)

                add_result = ("INSERT IGNORE INTO group_intro"
                                     "(group_url, group_name, group_founded_date, group_desc, group_sponsor, group_city, group_tags)"
                                     "VALUES (%s, %s, %s, %s, %s, %s, %s)")
                self.cursor.execute(add_result, args)
                self.dbconn.commit()




if __name__ == '__main__':
    crawler = crawl_group_intro()
    crawler.get_group_info()


