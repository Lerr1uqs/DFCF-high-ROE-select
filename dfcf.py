from bs4 import BeautifulSoup
import requests
import pdb
import pandas as pd
from rich.progress import track, Progress
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
import sys
import os
import tushare as ts
import akshare as ak
from rich.console import Console
from pathlib import Path
from typing import List
import pickle
from loguru import logger
import time

console = Console()

# 获取用户主目录
HOME = os.path.expanduser("~")

# 构建配置文件路径
TOEKN_PATH = os.path.join(HOME, ".tushare.token")

with open(TOEKN_PATH, "r") as file:
    token = file.read()

tsp = ts.pro_api(token)

STOCK_CODES_PKL_PATH = Path("storage") / Path("stock-codes.pkl")

if not os.path.exists(STOCK_CODES_PKL_PATH):
    with open(STOCK_CODES_PKL_PATH, "+wb") as f:
        pickle.dump(pd.DataFrame(), f)

with open(STOCK_CODES_PKL_PATH, "+rb") as f:
    CODES: pd.DataFrame = pickle.load(f)



chrome = webdriver.Chrome()

STORAGE_PKL_PATH = Path("storage") / Path("selected.pkl")

if not os.path.exists(STORAGE_PKL_PATH):
    with open(STORAGE_PKL_PATH, "+wb") as f:
        pickle.dump([], f)

with open(STORAGE_PKL_PATH, "+rb") as f:
    selected = pickle.load(f)

class CacheSelected:
    FILTER_PKL_PATH = Path("storage") / Path("filtered.pkl")

    def __init__(self, selected: List[pd.DataFrame]) -> None:
        self.se = selected
        self._codes = [s.loc[0, "CODE"] for s in selected]
        
        if not os.path.exists(CacheSelected.FILTER_PKL_PATH):
            with open(CacheSelected.FILTER_PKL_PATH, "+wb") as f:
                pickle.dump([], f)
                self.filtered: List[str] = []
        else:
            with open(CacheSelected.FILTER_PKL_PATH, "+rb") as f:
                self.filtered = pickle.load(f)
        
        with open(STORAGE_PKL_PATH, "+rb") as f:
            selected = pickle.load(f)

    @property
    def codes(self):
        return self._codes
    
    def append(self, df: pd.DataFrame):
        if df.loc[0, "CODE"] in self.codes:
            return
        
        self.se.append(df)
        self._codes.append(df.loc[0, "CODE"])

    def dump2pkl(self):

        with open(STORAGE_PKL_PATH, "+wb") as f:
            pickle.dump(self.se, f)
            
        with open(CacheSelected.FILTER_PKL_PATH, "+wb") as f:
            pickle.dump(self.filtered, f)
            
        logger.debug("flush2pkl triggered")


cs = CacheSelected(selected)

# 配置日志输出
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add("logfile.log", level="DEBUG", rotation="10 MB")

def run():

    if len(CODES.columns) == 0:
        stocks = ak.stock_info_a_code_name()
        with open(STOCK_CODES_PKL_PATH, "+wb") as f:
            pickle.dump(stocks, f)
    else:
        stocks = CODES

    with Progress() as progress:

        for index, row in progress.track(stocks.iterrows(), total=len(stocks)):
            code: str = row['code']
            name: str = row['name']

            if index % 300 in [1, 5, 20 ,50, 77]:
                cs.dump2pkl()
                # logger.debug(cs.se)

            # 更新进度条
            progress.update(progress.task_ids[0], description=f"fetching... {code}")
        
            URL = "https://quote.eastmoney.com/{}.html"

            # 上交所
            if any(code.startswith(head) for head in [
                    "60",  # a股
                    "730", # 新股
                    "900", # b股
                    "700", # 配股
                    "688", # 科创板
                    "787"  # 科创板新股
                ]):
                code = "sh" + code

            # 深交所
            elif any(code.startswith(head) for head in [
                    "00",  # a股 + 新股
                    "200", # b股
                    "080", # 配股
                    "002", # 中小板
                    "300"  # 创业板
                ]):
                code = "sz" + code
            # 北交所
            else:
                # 跳过
                logger.debug(f"skip 北交所")
                continue

            # 科创板 网页里没有这张表 
            if code[2:].startswith("688"):
                continue
            
            if code in cs.codes:
                # 已经加载过了
                logger.debug(f"{code} hit cache")
                continue

            if code in cs.filtered:
                logger.debug(f"{code} hit filtered")
                continue
            
            # logger.info(f"fetching {code}...")
            maxtry = 5

            while True:
                try:
                    time.sleep(0.1)
                    chrome.get(URL.format(code))
                    break
                except TimeoutException as te:
                    maxtry -= 1
                    if maxtry == 0:
                        raise te
                    
                    chrome.refresh()

            # content = requests.get(URL, headers=headers).text
            # print(content)
            soup = BeautifulSoup(chrome.page_source, "html.parser")
            # 在网页里右键检查可以看到
            table = soup.find("div", attrs={"class" : "finance4 afinance4"})
            if table is None:
                # REF: https://quote.eastmoney.com/kcb/688001.html
                # 这样的链接是找不到那张ROE与行业平均对比的表的
                logger.info(f"\n{code} can't found ROE table")
                continue

            # Extract column headers
            headers = [th.text.strip() for th in table.find('thead').find('tr').find_all('th')[1:]]

            # Extract table rows
            rows = []
            attr = {
                "name":"", 
                "field":"", 
                "rank":""
            }
            for idx, row in enumerate(table.find('tbody').find_all('tr')[:-1]): # 跳过最后一个四分位属性
                tds = row.find_all('td')

                if idx == 0:
                    attr["name"] = tds[0].find("a").string.strip()

                elif idx == 1:
                    attr["field"] = \
                        tds[0].find("div", attrs={"class":"hypj_hy"}) \
                              .find("a") \
                              .string \
                              .strip()
                            
                elif idx == 2:
                    attr["rank"] = tds[0].string.strip()
                    
                else:
                    raise RuntimeError

                row_data = [td.text.strip().replace("亿", "y").replace("万", "w") for td in tds[1:]]
                # print(row_data)
                rows.append([code] + row_data + [attr["name"], attr["field"]])

            # Create a DataFrame
            headers = [
                "CODE", # 股票代码
                "MC",  # market cap
                "NAV", 
                "NP", 
                "PE", 
                "PB", 
                "GPM", # Gross Profit Margin 
                "NPM", # Net Profit Margin
                "ROE",
                "NAME", # 名字
                "FLD", # field 领域
            ]
            df = pd.DataFrame(rows, columns=headers)

            # Display the DataFrame
            # print(df)
            '''
                CODE    MC    NAV      NP     PE     PB    GPM     NPM    ROE   NAME  FLD
            0 xxxxxx  1878y  4659y  396.4y   3.55   0.47  0.00%  31.05%  9.88%  茅台  消费
            1 xxxxxx  2566y  7577y  402.5y  4.750  0.530  0.00%  38.40%  9.41%  茅台  消费
            2 xxxxxx  12|42  15|42   10|42   2|42  17|42  42|42   36|42  14|42  茅台  消费
            '''
            # 获取ROE排名
            roe_rank: str = df.loc[2, "ROE"]
            devidend, dividor = map(int, roe_rank.split("|"))

            if "ST" in attr["name"]:
                logger.debug(f"skip {attr["name"]}")

            if devidend / dividor <= 0.20:
                cs.append(df)
                logger.debug(f"{df.loc[0, "CODE"]} be selected")
            else:
                cs.filtered.append(df.loc[0, "CODE"])
                logger.debug(f"{code} be filtered")


try:
    run()
    logger.info(f"get {len(cs.se)} roe-fine stocks")

except Exception as e:

    with open(STORAGE_PKL_PATH, "+wb") as f:
        pickle.dump(cs.se, f)
        
    console.print_exception(show_locals=False)
    raise e