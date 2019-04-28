
import time
import json
import requests
import MySQLdb
from functools import wraps
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup


class HttpCodeException(Exception):
    pass


def retry(retry_count=5, sleep_time=1):
    def wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            for i in range(retry_count):
                try:
                    res = func(*args, **kwargs)
                    return res
                except:
                    time.sleep(sleep_time)
                    continue
            return None
        return inner
    return wrapper





@retry()
def get_html(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.87 Mobile Safari/537.36',
        'Host': 'movie.douban.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Referer': 'https://movie.douban.com/top250?start=25&filter='
    }
    # 不使用代理的方法
    res = requests.get(url, headers=headers)
    time.sleep(3)   # 这里一定要sleep3秒,不然,频繁的抓取会导致IP被封
    print(res.status_code)
    if res.status_code != 200:
        raise HttpCodeException
    return res.text




def produce_url():
    """
    每个页面有25个电影，共10个页面，这10个页面的url可以自己生成
    :return:
    """
    url_style = "https://movie.douban.com/top250?start={index}&filter="
    url_lst = []
    #第二个参数应该调为250
    for i in range(0, 25, 25):
        url = url_style.format(index=i)
        #print(url)
        url_lst.append(url)

    return url_lst


def get_moive_info_url(page_url):
    """
    获取每个页面25个电影的详细信息的url
    :param page_url:
    :return:
    """
    html = get_html(page_url)
    soup = BeautifulSoup(html,"html.parser")

    moive_url_lst = []
    ol_node = soup.find('ol', class_='grid_view')
    pic_nodes = ol_node.find_all('div', class_='pic')
    for pic_node in pic_nodes:
        a = pic_node.find('a')
        href = a['href']
        moive_url_lst.append(href)

    return moive_url_lst


def get_describ_by_url(page_url):
    html = get_html(page_url)
    soup = BeautifulSoup(html,"html.parser")

    describ_list =[]
    ol_node = soup.find('ol',class_ ='grid_view')
    quote_nodes =ol_node.find_all('div',class_='info')
    for quote_node in quote_nodes:
        quo = quote_node.find('span',class_ ="inq")
        describ_list.append(quo.text)
    return describ_list




def run_single_thread():
    """
    单线程爬取
    :return:
    """
   # res_file = open('movie_data', 'w')
    t1 = time.time()
    moive_url_lst = []
    #describ_lst短评
    describ_lst =[]
    page_url_lst = produce_url()

    # moive_url_lst 存储250个电影的详细url
    for page_url in page_url_lst:
        url_lst = get_moive_info_url(page_url)
        describ = get_describ_by_url(page_url)
        moive_url_lst.extend(url_lst)
        describ_lst.extend(describ)


    for url in moive_url_lst:
        response = requests.get(url)
        soup = BeautifulSoup(response.text,"html.parser")
        title =soup.find('span',property="v:itemreviewed").text
        print(title)
        time.sleep(100)
        article =""


   # res_file.close()
    t2 = time.time()
    print("耗时" + str(t2-t1))



if __name__ == '__main__':
    db = MySQLdb.connect(host="localhost", user="root", passwd="root", db="blog", charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    delsql ='''DELETE  FROM top250'''
    cursor.execute(delsql)
    run_single_thread()
