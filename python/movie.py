import csv
import logging
import threading
import unicodedata

import requests
from bs4 import BeautifulSoup


class Model():
    count = 0

    def __init__(self):
        # 请求头
        self.headers = {
            'User-Agent': 'Mozilla/5.o (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/65.0.3325.162 Safari/537.36 '
        }
        # 存放每一步电影的id和imdb的id
        self.movie_dct = {}
        # 存放已经处理完的movie id
        self.white_lst = []
        # 电影详情的初始url
        self.url = 'https://www.imdb.com/title/'
        self.movie_csv_path = 'data/links.csv'
        # 电影信息的保存文件
        self.info_save_path = 'info.csv'
        # logging的配置，记录运行日志
        logging.basicConfig(filename="run.log", filemode="a+", format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S", level=logging.ERROR)
        # 表示当前处理的电影
        self.cur_movie_id = None
        self.cur_imdb_id = None

    def get_white_lst(self):
        '''获取处理完的白名单'''
        with open('white_list') as fb:
            for line in fb:
                line = line.strip()
                self.white_lst.append(line)

    def get_movie_id(self):
        '''获取电影的id和imdb的id'''
        with open(self.movie_csv_path) as fb:
            fb.readline()
            for line in fb:
                line = line.strip()
                line = line.split(',')
                # 电影id 对应 imdbid
                self.movie_dct[line[0]] = line[1]

    def update_white_lst(self, movie_id):
        '''更新白名单'''
        with open('white_list', 'a+') as fb:
            fb.write(movie_id + '\n')

    def update_black_lst(self, movie_id, msg=''):
        with open('black_list.txt', 'a+') as fb:
            # 写入movie id 和imdb id，并且加上错误原因
            # msg=1是URL失效，msg=2是电影没有海报
            fb.write(movie_id + ' ' + self.movie_dct[movie_id] + ' ' + msg + '\n')

    def get_url_response(self, url):
        '''访问网页请求，返回response'''
        logging.info(f'get {url}')
        i = 0
        # 超时重传，最多5次
        while i < 5:
            try:
                response = requests.get(url, timeout=6, headers=self.headers)
                if response.status_code == 200:
                    logging.info(f'get {url} sucess')
                    # 正常获取，直接返回
                    return response
                # 如果状态码不对，获取失败，返回None，不再尝试
                return None
            except requests.RequestException:
                # 如果超时
                i += 1
            logging.error(f'get {url} error')
        # 重试5次都失败，返回None
        return None

    def process_html(self, html):
        '''解析html，获取海报，电影信息'''
        soup = BeautifulSoup(html, 'lxml')
        # 电影名称
        name = soup.find('h1').get_text()
        # 去掉html的一些/x20等空白符
        name = unicodedata.normalize('NFKC', name)

        # 电影的基本信息   1h 21min | Animation, Adventure, Comedy | 21 March 1996 (Germany)
        info = []
        # 年份，G, 电影时长
        msg = soup.find_all('li', class_="ipc-inline-list__item")
        if msg[0].span is None:
            info.append(msg[0].get_text())
        else:
            info.append(msg[0].span.get_text())
        if msg[1].span is None:
            info.append(msg[1].get_text())
        else:
            info.append(msg[1].span.get_text())
        if msg[2].span is None:
            info.append(msg[2].get_text())
        else:
            info.append(msg[2].span.get_text())

        # 基本信息和详细发布时间 Animation, Adventure, Comedy | 21 March 1996 (Germany)
        for tag in soup.find_all(class_="ipc-chip__text"):
            info.append(tag.get_text().strip())
        info.pop(len(info) - 1)
        # 简介
        intro = soup.find(class_='GenresAndPlot__TextContainerBreakpointXS_TO_M-cum89p-0 dcFkRD').get_text().strip()
        intro = unicodedata.normalize('NFKC', intro)
        # 卡司。D W S C，分别表示 导演，编剧，明星，导演

        case_dict = {'D': [], 'W': [], 'S': []}

        msg = soup.find_all(class_="ipc-metadata-list-item__content-container")

        case_dict['D'].append(msg[0].get_text())
        for li in msg[1].ul:
            case_dict['W'].append(li.get_text())
        for writer in soup.find_all(class_="StyledComponents__ActorName-y9ygcu-1 eyqFnv"):
            case_dict['S'].append(writer.get_text())

        # id，电影名称，海报链接，时长，类型，发行时间，简介，导演，编剧，演员
        print(self.cur_movie_id)
        detail = [self.cur_movie_id, name, info[0], '|'.join(info[1:-1]),
                  info[-1], intro,
                  '|'.join(case_dict['D']), '|'.join(case_dict['W']), '|'.join(case_dict['S'])]
        self.save_info(detail)

    def save_info(self, detail):
        # 存储到CSV文件中
        with open(f'{self.info_save_path}', 'a+', encoding='utf-8', newline='') as fb:
            writer = csv.writer(fb)
            writer.writerow(detail)

    def run(self, num):
        # 开始爬取信息
        # 先读入文件
        self.get_white_lst()
        self.get_movie_id()
        movies = list(enumerate(self.movie_dct.items()))
        step = 9
        begin = num
        for i in range(begin, len(movies), step):
            if movies[i][1][0] in self.white_lst:
                continue
            self.cur_movie_id = movies[i][1][0]
            self.cur_imdb_id = movies[i][1][1]
            response = self.get_url_response(self.url + 'tt' + self.cur_imdb_id)

            if response is None:
                self.save_info([self.cur_movie_id, '' * 9])
                # 仍然更新白名单，避免重复爬取这些失败的电影
                self.update_white_lst(self.cur_movie_id)
                # 更新黑名单，爬完之后用另一个脚本再处理
                self.update_black_lst(self.cur_movie_id, '1')
                continue
            # 处理电影详情信息
            try:
                self.process_html(response.content)
            except TypeError as e:
                logging.info(f'get {self.cur_movie_id} sucess')
            # 处理完成，增加movie id到白名单中
            self.update_white_lst(self.cur_movie_id)
            logging.info(f'process movie {self.cur_movie_id} success')


if __name__ == '__main__':
    s = Model()
    s1 = Model()
    s2 = Model()
    s3 = Model()
    s4 = Model()
    s5 = Model()
    s6 = Model()
    s7 = Model()
    s8 = Model()

    threading.Thread(target=s.run, name=None, args=([0])).start()
    threading.Thread(target=s1.run, name=None, args=([1])).start()
    threading.Thread(target=s2.run, name=None, args=([2])).start()
    threading.Thread(target=s3.run, name=None, args=([3])).start()
    threading.Thread(target=s4.run, name=None, args=([4])).start()
    threading.Thread(target=s5.run, name=None, args=([5])).start()
    threading.Thread(target=s6.run, name=None, args=([6])).start()
    threading.Thread(target=s7.run, name=None, args=([7])).start()
    threading.Thread(target=s8.run, name=None, args=([8])).start()


