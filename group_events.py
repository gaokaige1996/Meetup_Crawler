import html5lib, requests
import mysql.connector
import random
from datetime import datetime, timedelta
import re
from bs4 import BeautifulSoup
from datetime import datetime

# database info
username = 'root'
password = '123456'
host = 'localhost'
dbase = 'meetup'

class crawl_group_events:

    def __init__(self):
        self.dbconn = mysql.connector.connect(user=username, password=password, host=host, database=dbase)
        self.cursor = self.dbconn.cursor()

    def get_group_events(self):

        select_sql = (
            "SELECT group_url, group_name, c_id from group_intro where c_id > 4199 order by c_id"
        )
        self.cursor.execute(select_sql)

        for cursor_data in self.cursor.fetchall():
            group_url = cursor_data[0]
            group_name = cursor_data[1]
            c_id = cursor_data[2]

            group_home_data = requests.get(group_url)
            group_soup = BeautifulSoup(group_home_data.text, 'html5lib')

            try:
                event_number = group_soup.select('.lastUnit.align-right')[3].get_text().strip()
                past_event_page_num = int(event_number) // 5 + 1
            except:
                try:
                    event_number = group_soup.select('.lastUnit.align-right')[2].get_text().strip()
                    past_event_page_num = int(event_number) // 5 + 1
                except:
                    try:
                        event_number = group_soup.select('.lastUnit.align-right')[1].get_text().strip()
                        past_event_page_num = int(event_number) // 5 + 1
                    except:
                        past_event_page_num = 1



            for event_page in range(0, past_event_page_num):

                group_event_url = '{}events/past/?page={}&__fragment=centerColMeetupList'.format(group_url, event_page)
                print(('group: {} event_page: {} past_event_page_num {}').format(group_name, event_page, past_event_page_num))

                wb_data = requests.get(group_event_url)

                event_urls = set(re.findall(r'{}events/\d+'.format(group_url), wb_data.text))

                for event_url in event_urls:

                    event_id = event_url.split('events/')[1]

                    event_wb_data = requests.get(event_url)
                    event_soup = BeautifulSoup(event_wb_data.text, 'html5lib')

                    try:
                        event_title = event_soup.select('#event-title > h1')[0].get_text().strip()
                        event_time = event_soup.select('#past-event-content > ul > li')[0].get_text().strip()
                        if "ago" in event_time:
                            day_time = event_time.split('·')[1]
                            today = datetime.today()
                            num = int(re.match(r'(\d)', event_time).group())
                            interval = timedelta(days=num)
                            event_time = (today - interval).strftime('%B %d, %y') + ' · ' + day_time
                        event_desc = event_soup.select('.line.margin-bottom')[0].get_text().strip()
                        event_mem_num = int(event_soup.select('#eventdets > div > div.lastUnit.event-attendee-section > div > div > h3 > span')[0].get_text().strip().replace(',',''))
                    except IndexError:
                        try:
                            event_title = event_soup.select('#event-title > h1')[0].get_text().strip()
                            event_time = event_soup.select('#past-event-content > ul > li')[0].get_text().strip()
                            if "ago" in event_time:
                                day_time = event_time.split('·')[1]
                                today = datetime.today()
                                num = int(re.match(r'(\d)', event_time).group())
                                interval = timedelta(days=num)
                                event_time = (today - interval).strftime('%B %d, %y') + day_time
                            event_desc = event_soup.select('.line.margin-bottom')[0].get_text().strip()
                            event_mem_num = int(event_soup.select(
                                '#eventdets > div > div.lastUnit.event-attendee-section > div > div > h3 > span')[
                                                    0].get_text().strip())
                        except IndexError:
                            pass

                    try:
                        mem_page_num = int(event_mem_num) // 50 + 1
                    except ValueError:
                        mem_page_num = 0

                    mem_id_set = set()

                    for mem_page in range(0, mem_page_num):

                        event_mem_url = '{}events/{}/?__fragment=past-attendee-list&p_attendeeList={}'.format(group_url, event_id, mem_page)

                        mem_wb_data = requests.get(event_mem_url)

                        mem_id = set(re.findall(r'{}members/(\d+)'.format(group_url), mem_wb_data.text))

                        mem_id_set = mem_id_set.union(mem_id)

                    print(c_id, group_name, group_url, group_event_url, event_url)

                    args = (group_name, group_url, event_id, event_time, event_title, event_desc, str(mem_id_set), event_mem_num)

                    add_result = ("INSERT IGNORE INTO group_events"
                                  "(group_name, group_url, event_id, event_time, event_title, event_desc, mem_id_set, event_mem_num)"
                                  "VALUES (%s, %s, %s, %s, %s, %s, %s ,%s)")

                    self.cursor.execute(add_result, args)
                    self.dbconn.commit()


if __name__ == '__main__':
    crawler = crawl_group_events()
    crawler.get_group_events()


