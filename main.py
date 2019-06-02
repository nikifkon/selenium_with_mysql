from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import pymysql
from pymysql.cursors import DictCursor
from contextlib import closing
import datetime
from multiprocessing import Pool
from time import time


class Hotline:
    max_process = 6
    host = "localhost"
    port = 3306
    user = "root"
    password = ""
    db = "hotline"
    charset = "utf8mb4"

    test = "https://hotline.ua/profile/lists/get-all-lists/"
    data = []
    url_list = []
    test_url_list = ["https://hotline.ua/computer-besprovodnoe-oborudovanie/ubiquiti-unifi-ac-pro-ap-uap-ac-pro/",
                     "https://hotline.ua/computer-besprovodnoe-oborudovanie/tp-link_tl-wr841n/",
                     "https://hotline.ua/computer-besprovodnoe-oborudovanie/tenda-ac10u/",
                     "https://hotline.ua/computer-besprovodnoe-oborudovanie/tp-link-archer-c6/",
                     "https://hotline.ua/computer-besprovodnoe-oborudovanie/xiaomi-mi-wifi-router-3/"]
    black_list = ['Твій Дім', 'Техноскарб', 'МУЛЬТИМЕДІА']

    def get_urls(self):
        with closing(pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                db=self.db,
                charset=self.charset,
                cursorclass=DictCursor
        )) as con:
            with con.cursor() as cursor:
                query = "SELECT `url` FROM `urls`"
                cursor.execute(query)
                print(f"Выполнен запрос {query} к базе данных")
                con.commit()
                for row in cursor:
                    self.url_list.append(row["url"])

    def write_mysql(self, value):

        with closing(pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                db=self.db,
                charset=self.charset,
                cursorclass=DictCursor
        )) as con:
            with con.cursor() as cursor:
                query = f"INSERT INTO `data` (`url`, `name`, `max`, `min`, `average`, `date`) VALUES {value}"
                cursor.execute(query)
                print(f"Выполнен запрос {query} к базе данных")
                con.commit()
            # with con.cursor() as cursor:
            #     query = "SELECT * FROM `url`"
            #     cursor.execute(query)
            #     for row in cursor:
            #         print(row)

    def selenium(self, url):
        local_data = []
        driver = webdriver.PhantomJS()  # git repo http://bit.ly/2QEljHc
        driver.get(url)
        for i in range(1):
            try:
                button = driver.find_element_by_xpath("//li[@data-id='prices']")
                button.click()
                break
            except:
                driver.find_element_by_xpath("//div[@class='lightbox']//i[@class='close']").click()
                continue
        try:
            # wait until element .hidden viewbox....#js-price-sort appear in DOM
            WebDriverWait(driver, 30) \
                .until(EC.presence_of_element_located((By.XPATH,
                       "//div[@class='hidden viewbox all-offers']//div[@id='js-price-sort']")))
        finally:
            lines = driver. \
                find_elements_by_xpath("//div[@class='hidden viewbox all-offers']//div[@data-selector='price-line']")
        title = driver.find_element_by_xpath("//h1[@datatype='card-title']").text
        print(f"Товар : {title}")
        # parse loop
        for line in lines:
            a = False
            shop = line.find_element_by_xpath(".//div[@class='shop-box']")  # .// for get child element( of line)
            # for more info http://bit.ly/2JPRMtK
            shop2 = shop.find_element_by_xpath(".//div[@class='shop-box-in cell-7 cell-md']")
            name = shop2.find_element_by_xpath(".//div[@class='ellipsis']").text
            # checks
            if name == "Rozetka.ua":
                try:
                    product = line.find_element_by_xpath(".//div[@class='product-box']")
                    seller = product.find_element_by_xpath(".//ins").text
                except:
                    seller = 'нет'
            else:
                seller = 'Не Розетка'

            for black in self.black_list:
                if name in black or black in name or name == black:
                    a = True
            if a:
                continue

            prise_container = line.find_element_by_xpath(".//div[@class='price-box row']")
            prise_value = prise_container.find_element_by_xpath(".//span[@class='value']").text.replace(' ', '')
            prise_penny = prise_container.find_element_by_xpath(".//span[@class='penny']").text.replace(' ', '')
            local_data.append(int(prise_value))

            print(f'{len(local_data)} : Имя - {name}, продавец - {seller}, цена - {prise_value}{prise_penny}')

        max_price = int(max(local_data))
        min_price = int(min(local_data))
        average = sum(local_data) / len(local_data)
        average = round(average)
        self.data.append({'title': title,
                          'max': max_price,
                          'min': min_price,
                          'average': average,
                          'url': url,
                          'date': str(datetime.date.today())})

        value = (url, title, max_price, min_price, average, str(datetime.date.today()), )
        self.write_mysql(value)
        driver.quit()

    def run(self):
        self.get_urls()
        print(self.url_list)
        if len(self.url_list) < self.max_process:
            with Pool(len(self.url_list)) as p:
                p.map(self.selenium, self.url_list)
        else:
            with Pool(self.max_process) as p:
                p.map(self.selenium, self.url_list)


if __name__ == "__main__":
    start = time()

    Hotline().run()

    end = time()
    total = end - start

    print(total)
    print(f"Время выполнения {int(total // 60)} мин. и {round(total % 60)} сек.")
