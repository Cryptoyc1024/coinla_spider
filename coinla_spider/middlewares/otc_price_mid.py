# -*- coding: utf-8 -*-

import logging
import re

from redis import StrictRedis
from scrapy.exceptions import IgnoreRequest
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, \
    WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ..settings import CACHE_HOST, CACHE_PORT, CACHE_PASSWORD


class OTCPriceDownloaderMiddleware(object):

    def process_response(self, request, response, spider):
        if 'coincola' in response.url:
            resp_text = response.body.decode(response.encoding)
            if 'timeout' in resp_text:
                response.status = 404
        return response


class OKEXOTCPriceMiddleware(object):

    def __init__(self):
        self.cache_cli = StrictRedis(CACHE_HOST, CACHE_PORT, password=CACHE_PASSWORD,
                                     max_connections=10, decode_responses=True,
                                     socket_timeout=1, socket_connect_timeout=1)

    def process_request(self, request, spider):
        if 'okex' not in request.url:
            return None

        auth = self.cache_cli.get('Auth:OKEX')
        if auth is None:
            d = DesiredCapabilities.CHROME
            d['loggingPrefs'] = {'performance': 'ALL'}
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            driver = webdriver.Chrome(desired_capabilities=d, chrome_options=options)
            driver.implicitly_wait(20)
            driver.get('https://www.okex.com/otc')
            wait = WebDriverWait(driver, 30)
            try:
                wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'dialog-confirm-btn')))
                driver.find_element_by_class_name('dialog-confirm-btn').click()
                driver.find_element_by_xpath('//span[@data-type="1"]').click()
                driver.find_element_by_name('username').clear()
                driver.find_element_by_name('username').send_keys('zkqiang@hainan.com')
                driver.find_element_by_name('password').clear()
                driver.find_element_by_name('password').send_keys('coin2018')
                driver.find_element_by_xpath('//button[@class="login-btn "]').click()
                wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'pair')))
                for log in driver.get_log('performance'):
                    msg = log['message']
                    if 'https://www.okex.com/v3/users/security/profile' in msg:
                        auth = re.findall(r'"Authorization":"(.+?)"', msg)[0]
                        self.cache_cli.set('SpiderCache:Auth:OKEX', auth)
                        break
            except (NoSuchElementException, TimeoutException, WebDriverException):
                logging.error('Selenium运行出错，未获得Auth')
                raise IgnoreRequest()
            except IndexError:
                logging.error('未从返回参数中获得Auth')
                raise IgnoreRequest()
            finally:
                driver.quit()
        request.headers['authorization'] = auth

    def process_response(self, request, response, spider):
        if 'okex' in request.url and response.status == 403:
            self.cache_cli.delete('SpiderCache:Auth:OKEX', 'OKEX')
            return request.replace()
        return response
