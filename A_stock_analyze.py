"""
@Author         :  JinMing Xie
@Version        :  Python 3.7.3
——————————————————
@File           :  China_stock_hs.py
@Description    :
@CreateTime     :  2020/01/12
——————————————————
@ModifyTime     :
"""
# Ajax类型  东方财富网（沪深个股）
import re
import pymysql
import requests
from lxml import html
from selenium import webdriver
from multiprocessing import Process
# 数据库
# CREATE TABLE `data2` (
#   `number` varchar(10) NOT NULL,
#   `name` varchar(10) NOT NULL,
#   `time` varchar(10) DEFAULT NULL,
#   `price` float(10,3) DEFAULT NULL,
#   `PE` float(10,2) DEFAULT NULL,
#   `PE_rank` int(10) DEFAULT NULL,
#   `ROE` float(10,2) DEFAULT NULL,
#   `ROE_rank` int(10) DEFAULT NULL,
#   `HY_ROE` float(10,2) DEFAULT NULL,
#   `value` float(10,2) DEFAULT NULL,
#   `HY` varchar(10) DEFAULT NULL,
#   `rank` int(10) DEFAULT NULL,
#   PRIMARY KEY (`number`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

class GetData:
    """数据的获取"""
    def __init__(self):
        self.headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 '
                    '(KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
        }

    def get_page(self, url):
        """获取网页源码"""
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.text
        except requests.ConnectionError as e:
            print('Error', e.args)

    def get_page2(self, url, regex1=None, regex2=None, regex3=None, regex4=None):
        """登录后获取roe等数据"""
        try:
            chromeOptions = webdriver.ChromeOptions()
            chromeOptions.add_argument('headless')
            chromeOptions.add_experimental_option('excludeSwitches', ['enable-automation'])  # 防止网站发现我们使用模拟器
            self.browser = webdriver.Chrome(options=chromeOptions)
            self.browser.get(url)
            etree = html.etree
            a = etree.HTML(self.browser.page_source)
            roe = a.xpath(regex1)
            hy_roe = a.xpath(regex2)
            hy = a.xpath(regex3)
            value = a.xpath(regex4)
            self.browser.quit()
            return [roe, hy_roe, hy, value]
        except Exception:
            self.browser.quit()
            print('请求错误')

    def get_stock_data(self, text):
        """获取股票代码、名称、PE"""
        com = re.compile('"f2":(?P<price>.+?),.*?"f9":(?P<PE>.+?),.*?"f12":"(?P<number>.+?)",.*?'
                         '"f14":"(?P<name>.+?)",', re.S)
        ret = com.finditer(text)
        for i in ret:
            yield {
                'number': i.group('number'),
                'name': i.group('name'),
                'price': i.group('price'),
                'PE': i.group('PE')
            }

    def judge_zone(self, number=None):
        """根据股票代码判断上市的证交所"""
        number = str(number)
        if number[0:2] == '60':
            zone = 'sh'
        if number[0:2] == '00' or number[0:3] == '300':
            zone = 'sz'
        if number[0:3] == '688':
            print('科创板')
        # try:
        self.get_roe_hy(zone=zone, number=number)
        # except Exception:
        #     print('地址出错')

    def get_roe_hy(self, zone=None, number=None):
        """
        获取对应股票的ROE,行业平均ROE
        从数据库中提取股票代码，再进行特定股票网页的爬取
        """
        zone_number = zone + str(number)
        # 个股URL http://quote.eastmoney.com/(sz或sh股票代码).html
        stock_url = 'http://quote.eastmoney.com/%s.html' % (str(zone_number))
        print(stock_url)
        try:
            # 获取ROE
            roe = '//*[@id="cwzbDataBox"]/tr[1]/td[9]/text()'
            # 获取行业ROE
            hy_roe = '//*[@id="cwzbDataBox"]/tr[2]/td[9]/text()'
            # 获取该只股票所在的行业
            hy = '//*[@id="cwzbDataBox"]/tr[2]/td[1]/a/text()'
            # 获取市值
            value = '//*[@id="cwzbDataBox"]/tr[1]/td[2]/text()'
            data = self.get_page2(stock_url, regex1=roe, regex2=hy_roe, regex3=hy, regex4=value)
            # 存入数据
            try:
                if data[0][0] == '"-"' or data[1][0] == '"-"' or data[2][0] == '"-"' or data[3][0] == '"-"':
                    roe, hy_roe, hy, value = '0', '0', '0', '0'
                    SaveData().input_data(update_roe=True, number=number, roe=roe, hy_roe=hy_roe, hy=hy, value=value)
                SaveData().input_data(update_roe=True, number=number, roe=data[0][0][:-1], hy_roe=data[1][0][:-1],
                                      hy=data[2][0], value=data[3][0][:-1])
            except TypeError:
                print('数据没有获取到！')
            if roe and hy_roe and hy and value:
                print(str(number) + '数据插入成功！')
            else:
                print(str(number) + '数据插入失败！')
        except IndexError:
            print(str(number) + '数据插入失败！')
        print(str(self.achieve) + '/' + str(len(SaveData().output_data(number=True))))  # 显示进度

    def _main(self, update=False, insert=False):
        save_data = SaveData()
        for i in range(1, 198):
            url = 'http://60.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112408744624686429123_1578798932591&pn=' \
                  '%d&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13,m:' \
                  '0+t:80,m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,' \
                  'f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1583923192934' % i
            content = self.get_page(url=url)
            data = self.get_stock_data(content)
            for j in data:
                number = j.get('number')
                name = j.get('name')
                price = j.get('price')
                pe = j.get('PE')
                if insert:
                    if pe == '"-"' or price == '"-"':
                        price, pe = '0', '0'
                    save_data.input_data(insert_data=True, number=number, name=name, price=eval(price), pe=eval(pe))
                elif update:
                    if pe == '"-"' or price == '"-"':
                        price, pe = '0', '0'
                    save_data.input_data(update_price=True, number=number, name=name, price=eval(price), pe=eval(pe))
        save_data.rinse_data()
        self.start_process()

    def num(self, n=None):
        self.achieve = 0
        for i in n:
            self.achieve += 1
            self.judge_zone(number=i)

    def start_process(self):
        """启动进程"""
        save_data = SaveData()
        processes = []
        number = []
        index = 0
        for i in range(len(save_data.output_data(number=True))):
            print(i)
            number.append(save_data.output_data(number=True)[i][0])
        for i in range(3):
            print('启动第%d进程' %(i))
            p = Process(target=self.num, args=(number[index:index + 1350],))
            processes.append(p)
            index += 1350
            p.start()
        for p in processes:
            p.join()

    def first_insert(self):
        """第一次插入数据"""
        self._main(insert=True)

    def update_data(self):
        """更新代码、名称、每股价格、数据"""
        self._main(update=True)


class SaveData:
    """数据的进出"""
    def __init__(self):
        self.db = pymysql.connect("localhost", "root", "17322080357a", "stock_data")
        self.cursor = self.db.cursor()

    def input_data(self, update_pe_rank=False, update_roe_rank=False, update_rank=False, update_price=False,
                   insert_data=False, update_roe=False, number=None, name=None, price=None, pe=None, roe=None,
                   hy_roe=None, hy=None, value=None, pe_rank=None, roe_rank=None, rank=None):
        """存储数据进入MySQL"""
        if update_roe:   # 更新roe,市值等数据
            sql = "UPDATE data2 SET ROE = %.2f, HY_ROE = %.2f, HY = '%s', value = %.2f WHERE number = '%s'" \
                  % (eval(roe), eval(hy_roe), hy, eval(value), number)
        elif update_pe_rank:   # 更新pe的排名
            sql = "UPDATE data2 SET PE_rank = %d WHERE number = '%s'" % (pe_rank, number)
        elif update_roe_rank:   # 更新roe的排名
            sql = "UPDATE data2 SET ROE_rank = %d WHERE number = '%s'" % (roe_rank, number)
        elif update_rank:   # 更新股票排名
            sql = "UPDATE data2 SET rank = %d WHERE number = '%s'" % (rank, number)
        elif update_price:  # 插入代码、名称、每股价格、数据
            sql = "UPDATE data2 SET number='%s', name='%s', price=%.2f, pe=%.2f" % (number, name, price, pe)
        elif insert_data:   # 插入代码、名称、每股价格、数据
            sql = "INSERT INTO data2 (number, name, price, pe)VALUES ('%s', '%s', %.2f, %.2f)" \
                  % (number, name, price, pe)
        self.cursor.execute(sql)
        self.db.commit()

    def rinse_data(self):
        """
        剔除科创板、新股、*ST、ST、PT、S开头的股票
        剔除价格为--的股票
        剔除ROE、PE为负的
        """
        # 剔除科创板股票
        sql = "DELETE FROM data2 WHERE number like '688%'"
        self.cursor.execute(sql)

        # 剔除*ST、ST、PT、S开头的股票
        sql = "DELETE FROM data2 WHERE name like '*ST%' "
        self.cursor.execute(sql)
        sql = "DELETE FROM data2 WHERE name like 'ST%'"
        self.cursor.execute(sql)
        sql = "DELETE FROM data2 WHERE name like 'PT%'"
        self.cursor.execute(sql)
        sql = "DELETE FROM data2 WHERE name like 'S%'"
        self.cursor.execute(sql)

        # 剔除PE为负的
        sql = "DELETE FROM data2 WHERE PE <= 0"
        self.cursor.execute(sql)

        # 剔除ROE为负的
        sql = "DELETE FROM data2 WHERE ROE <= 0"
        self.cursor.execute(sql)

        self.db.commit()
        print('数据清洗完成！')

    def output_data(self, number=False, roe=False, hy_roe=False, pe=None, value=False, all=False):
        """提取数据"""
        if roe and hy_roe:
            sql = "SELECT number, PE, ROE, HY_ROE FROM data2"
        elif number:
            sql = "SELECT number FROM data2"
        elif pe:
            sql = "SELECT number, PE FROM data2"
        elif roe:
            sql = "SELECT number, ROE FROM data2"
        elif value:
            sql = "SELECT price, value FROM data2 ORDER BY rank ASC limit 30"
        elif all:   # 输出的结果按rank ASC排序 挑选前30名
            sql = "SELECT * FROM data2 ORDER BY rank ASC limit 30"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()    # 元组加元组
        return list(results)


class Analyze:
    """数据的分析"""
    def ranking(self):
        """结合ROE、PE进行排名"""
        # 计算pe的排名
        self.save_data = SaveData()
        number_pe = self.save_data.output_data(pe=True)
        pe_sort = sorted(number_pe, key=lambda x: (x[1]))   # 对pe进行排序
        print(pe_sort)
        for i in range(len(self.save_data.output_data(number=True))):
            pe_rank = pe_sort.index(number_pe[i])
            self.save_data.input_data(update_pe_rank=True, pe_rank=pe_rank+1, number=number_pe[i][0])

        # 计算roe的排名
        number_roe = self.save_data.output_data(roe=True)
        roe_sort = sorted(number_roe, key=lambda x: (x[1]))
        print(roe_sort[::-1])
        for i in range(len(self.save_data.output_data(number=True))):
            roe_rank = roe_sort[::-1].index(number_roe[i])
            self.save_data.input_data(update_pe_rank=True, roe_rank=roe_rank+1, number=number_roe[i][0])

        # # 结合pe和roe的排名进行排名
        sql = "SELECT number, PE_rank, ROE_rank FROM data2"
        self.save_data.cursor.execute(sql)
        result = self.save_data.cursor.fetchall()
        print(result)
        for i in result:
            number = i[0]
            pe_rank = i[1]
            roe_rank = i[2]
            rank = pe_rank + roe_rank   # 得出股票排名
            self.save_data.input_data(update_rank=True, rank=rank, number=number)

    def write_file(self):
        """将结果写入文件"""
        f = open('../stock_data.txt', 'a', encoding='utf8')
        reslut = self.save_data.output_data(all=True)
        for i in reslut:
            f.write(str(i) + "\n")

    def invest(self):
        """根据公司市值进行投资"""
        value = []
        result = self.save_data.output_data(value=True)
        for i in result:
            value.append(i[0])
        total_value = sum(value)
        percent = []
        for i in result:
            percent.append(i[0] / total_value)
        invest = []
        for i in percent:
            invest.append(300000 * i)
        amount = []
        price = []
        for i in result:
            price.append(i[0])
        for i in range(len(invest)):
            amount.append(invest[i] // price[i])
        print(amount)


def main():
    analyze = Analyze()
    get_data = GetData()
    save_data = SaveData()
    get_data.first_insert()   # 第一次插入数据
    # get_data.update_data()  # 更新数据
    analyze.ranking()   # 计算排名
    analyze.write_file()    # 将符合条件的写入文本
    analyze.invest()    # 计算买入的股数
    print('成功完成！')


if __name__ == '__main__':
    main()
