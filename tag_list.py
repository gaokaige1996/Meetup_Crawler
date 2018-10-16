import html5lib, requests
import mysql.connector
import random
import time
import re
from bs4 import BeautifulSoup


# database info
username = 'root'
password = '123456'
host = 'localhost'
dbase = 'meetup'


class crawl_tag:

    def __init__(self):
        self.dbconn = mysql.connector.connect(user=username, password=password, host=host, database=dbase)
        self.cursor = self.dbconn.cursor()

    def get_tag_list(self):

        topic_url = "https://www.meetup.com/topics/"
        wb_data = requests.get(topic_url)

        soup = BeautifulSoup(wb_data.text, 'html5lib')
        tech_tag = soup.select('.gridList-item')

        i = 0
        for temp_info in tech_tag[1851:2033]:
            i += 1
            tag_name = temp_info.select('.link')[0].get_text()
            tag_url = temp_info.select('.link')[0]['href']
            print(tag_name, tag_url, i)

            args = (tag_name,tag_url)
            add_result = ("INSERT IGNORE INTO tag_list"
                                 "(tag_name, tag_url)"
                                 "VALUES (%s, %s)")
            self.cursor.execute(add_result, args)
            self.dbconn.commit()

if __name__ == '__main__':
    crawler = crawl_tag()
    crawler.get_tag_list()