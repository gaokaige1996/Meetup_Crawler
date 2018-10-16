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


class crawl_all_groups:

    def __init__(self):
        self.dbconn = mysql.connector.connect(user=username, password=password, host=host, database=dbase)
        self.cursor = self.dbconn.cursor()

    def get_group_info(self):
#筛选出name 和url
        select_sql = (
            "SELECT tag_name, tag_url FROM tag_list"
        )
        self.cursor.execute(select_sql)

        group_id = ''
        # fetch all of the rows from the query
        for cursor_data in self.cursor.fetchall():
            tag_name = cursor_data[0]
            tag_url = cursor_data[1]
#为啥加all
            group_url = tag_url + 'all'

            wb_data = requests.get(group_url)
            soup = BeautifulSoup(wb_data.text, 'html5lib')#html5lib是解析器

            groups_info = soup.select('.gridList-item')

            for group_info in groups_info:

                try:

                    group_name = group_info.select('.text--bold')[0].get_text()
                    temp_info = group_info.select('.text--small')[0].get_text().strip()
                    group_member = temp_info.split('|')[0].split(' ')[0]
#如果tag只有一个NavigableString类型子节点，则可用`.string`获取。如果包含多个，使用`.strings`遍历。若输出的字符串中包含空格或空行，使用`.stripped_strings`去除。
                    group_url = group_info.select('a')[0]['href']

                    group_loc = temp_info.split('|')[1].split(',')[1].strip()
                    group_city = temp_info.split('|')[1].split(',')[0].strip()
                except Exception as err:
                    print(err)

                if len(group_loc) == 2:
                    group_state = group_loc
                    group_country = "United States"
                else:
                    group_state = ''
                    group_country = group_loc

                print(tag_name, group_name, group_member, group_city, group_state, group_country, group_url)

                args = (tag_name, group_name, group_member, group_city, group_state, group_country, group_url)

                add_result = ("INSERT IGNORE INTO all_groups"
                                     "(tag_name, group_name, group_member, group_city, group_state, group_country, group_url)"
                                     "VALUES (%s, %s, %s, %s, %s, %s, %s)")
                self.cursor.execute(add_result, args)
                self.dbconn.commit()

#为什么是——name == main——， main是啥？~~
if __name__ == '__main__':
    crawler = crawl_all_groups()
    crawler.get_group_info()


