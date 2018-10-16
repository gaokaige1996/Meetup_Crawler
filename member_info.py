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

    def get_member_info(self):

        select_sql = (
            "SELECT group_url, group_name, c_id from group_intro where group_url not in (SELECT distinct(group_url) FROM meetup.member_info) and c_id > 1951 order by c_id"
        )
        self.cursor.execute(select_sql)

        for cursor_data in self.cursor.fetchall():
            group_url = cursor_data[0]
            group_name = cursor_data[1]
            c_id = cursor_data[2]

            all_members = group_url + 'members/'

            try:
                wb_data = requests.get(all_members)
            except :
                time.sleep(100)
                continue

            soup = BeautifulSoup(wb_data.text, 'html5lib')


            try:
                member_number = soup.select('.lastUnit.align-right')[0].get_text().strip().replace(',', '')
                crawl_flag = 1
            except IndexError:
                crawl_flag = 0
                print('Wrong Page')

            if crawl_flag == 1:

                page_number = int(member_number) // 20 + 1

                for page in range(0, page_number):

                    memember_page = all_members + '?offset={}&sort=last_visited&desc=1'.format(page*20)

                    print(memember_page)

                    try:
                        mem_list_wb_data = requests.get(memember_page)
                    except:
                        time.sleep(50)
                        continue

                    mem_list_soup = BeautifulSoup(mem_list_wb_data.text, 'html5lib')

                    temp_members = mem_list_soup.select('.flush--bottom.inlineBlock')

                    for temp_member in temp_members:

                        member_url = temp_member.select('.memName')[0]['href']

                        member_id = member_url.split('members/')[1].split('/')[0]
                        member_name = temp_member.select('.memName')[0].get_text()

                        try:
                            member_data = requests.get(member_url)
                        except:
                            time.sleep(100)
                            continue
                        member_soup = BeautifulSoup(member_data.text, 'html5lib')

                        try:
                            member_city = member_soup.select('.locality')[0].get_text().strip()
                            member_state = member_soup.select('.region')[0].get_text().strip()
                        except IndexError:
                            member_city = ''
                            member_state = ''
                        try:
                            member_soup.select('.text--secondary')[1].get_text()
                            member_org = 'Yes'
                            joined_date = member_soup.select('#D_memberProfileMeta > div > p')[0].get_text()
                        except IndexError:
                            try:
                                member_org = 'No'
                                joined_date = member_soup.select('#D_memberProfileMeta > div > div > p')[1].get_text()
                            except IndexError:
                                pass


                        try:
                            picture_url = member_soup.select('#member-profile-photo > a')[0]['href']
                        except IndexError:
                            picture_url = ''

                        try:
                            member_interests = member_soup.select('.D_group.small.last')[0].get_text().replace('\n', '').replace('\t', '').strip()
                        except IndexError:
                            member_interests = ''

                        member_groups = []

                        groups_info = member_soup.select('.figureset-figure')

                        for temp_group in groups_info:
                            mem_grp_name = temp_group.select('.omnCamp')[0]['title'].replace('Meetup Group: ', '')
                            mem_grp_link = temp_group.select('.omnCamp')[0]['href']
                            group_info = mem_grp_name + " : " + mem_grp_link
                            member_groups.append(group_info)

                        print(c_id, member_url, member_id, member_name, joined_date, member_city, page)

                        args = (member_id, member_name, member_url, group_url, group_name, joined_date, picture_url, member_interests, str(member_groups), member_city, member_state, member_org)

                        add_result = ("INSERT IGNORE INTO member_info"
                                      "(member_id, member_name, member_url, group_url, group_name, joined_date, picture_url, member_interests, member_groups, member_city, member_state, member_org)"
                                      "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

                        self.cursor.execute(add_result, args)
                        self.dbconn.commit()


if __name__ == '__main__':
    crawler = crawl_group_intro()
    crawler.get_member_info()


