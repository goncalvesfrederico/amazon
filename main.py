import pandas as pd
import ast
import time
import requests
import random
import pymongo
import datetime as dt
from bs4 import BeautifulSoup

conn_str = f"mongodb://.../"
mongo_client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS = 5000)

vcookies = {
    'aws-target-data': '%7B%22support%22%3A%221%22%7D',
    'regStatus': 'pre-register',
    'AMCV_7742037254C95E840A4C98A6%40AdobeOrg': '1585540135%7CMCIDTS%7C19222%7CMCMID%7C85751116849906783270115487189627206170%7CMCAAMLH-1661360755%7C4%7CMCAAMB-1661360755%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1660763156s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C4.4.0',
    'aws-target-visitor-id': '1660755955828-488368.34_0',
    'session-id': '138-6912255-0767005',
    'session-id-time': '2082787201l',
    'i18n-prefs': 'USD',
    'sp-cdn': '"L5Z9:BR"',
    'ubid-main': '135-0855832-7785139',
    'lc-main': 'en_US',
    'session-token': 'ZSDrcmyKAtOiTbgo8RtBVo/u231H6i9tJ0g0oVqMW4r6nyRU5aK65NtL3Pj3a5CSsleABF9ZzYbm3+X6lW7mqno1SdhoAq/ZTYExWXYvMVS8UUs7BoCvAD7CDwwPwhqV+Se/aeT9bhWsSNy/FP0Wc0DQJAydTGUY6ycWt5VhBzcYYOms+W2WK9TlhND47yVMky5+K4ym+c7OYl1VQEnEE8VkbZH94ZJP',
    'skin': 'noskin',
    'csm-hit': 'tb:s-VQBHYZ72BX0WSR2SXHRP|1665451251744&t:1665451252254&adb:adblk_no',
}

vheaders = {
    'authority': 'www.amazon.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    # Requests sorts cookies= alphabetically
    # 'cookie': 'aws-target-data=%7B%22support%22%3A%221%22%7D; regStatus=pre-register; AMCV_7742037254C95E840A4C98A6%40AdobeOrg=1585540135%7CMCIDTS%7C19222%7CMCMID%7C85751116849906783270115487189627206170%7CMCAAMLH-1661360755%7C4%7CMCAAMB-1661360755%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1660763156s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C4.4.0; aws-target-visitor-id=1660755955828-488368.34_0; session-id=138-6912255-0767005; session-id-time=2082787201l; i18n-prefs=USD; sp-cdn="L5Z9:BR"; ubid-main=135-0855832-7785139; lc-main=en_US; session-token=ZSDrcmyKAtOiTbgo8RtBVo/u231H6i9tJ0g0oVqMW4r6nyRU5aK65NtL3Pj3a5CSsleABF9ZzYbm3+X6lW7mqno1SdhoAq/ZTYExWXYvMVS8UUs7BoCvAD7CDwwPwhqV+Se/aeT9bhWsSNy/FP0Wc0DQJAydTGUY6ycWt5VhBzcYYOms+W2WK9TlhND47yVMky5+K4ym+c7OYl1VQEnEE8VkbZH94ZJP; skin=noskin; csm-hit=tb:s-VQBHYZ72BX0WSR2SXHRP|1665451251744&t:1665451252254&adb:adblk_no',
    'device-memory': '8',
    'downlink': '8.25',
    'dpr': '1',
    'ect': '4g',
    'rtt': '150',
    'sec-ch-device-memory': '8',
    'sec-ch-dpr': '1',
    'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-viewport-width': '1366',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
    'viewport-width': '1366',
}

def fn_readfilecsv():
    
    initialline = 220
    finalline = 235

    print(f"Reading lines {initialline} to {finalline}")

    filename = "C:/ProjetosPython/flexcodellc/all_domains.csv"
    input_domain = pd.read_csv(f"{filename}")["Domain"].str.replace("https://", "").str.replace("http://", "").str.strip()
   
    return input_domain[initialline:finalline]


def fn_concatenatedomains() -> list:

    domains = fn_readfilecsv()
    domains = [website for website in domains]
    domains = str(domains).replace(".com", "").replace(".net", "").replace(".org","").replace(".co.uk", "").replace(".fr", "").replace(".com.au", "")
    domains = domains.replace(".top", "").replace(".ca", "").replace(".au", "").replace(".xyz", "").replace(".ml", "").replace(".co.jp", "")
    domains = domains.replace(".nl", "").replace(".it", "")
    listdomains = ast.literal_eval(domains)
    return listdomains

domainsfound = []

def fn_get_products_and_urls() -> list:
    
    listdomains = fn_concatenatedomains()

    for domain in listdomains:
        base_url = f"https://www.amazon.com/s?k={domain}"
        print(f"Processing {base_url}...")

        response = requests.get(base_url, cookies=vcookies, headers=vheaders)
        html = BeautifulSoup(response.text, "html.parser")
        
        try:
            name_results = html.find("div", {"class": "a-section a-spacing-small a-spacing-top-small"})
            name_results_text = name_results.find("span", {"class": "a-color-state a-text-bold"}).text
            name_results_text = name_results_text.replace('"',"")
            name_results_bool = bool(name_results_text)
            # print(name_results_text, name_results_bool)
        except:
            try:
                name_results = html.find("span", {"data-component-type": "s-result-info-bar", "class": "rush-component"})
                name_results = name_results.find("div", {"class": "sg-col-inner"})
                name_results = name_results.find("span", {"class": "a-color-state"}).text
                name_results_text = name_results_text.replace('"', "")
                name_results_bool = bool(name_results_text)
            except:
                try:
                    name_results = html.find("span", {"data-component-type": "s-search-results", "class": "rush-component"})
                    name_results = name_results.find("div", {"class": "a-section", "data-cel-widget": "search_result_0"})
                    name_results_text = name_results.find("span", {"class": "a-size-medium a-text-italic"}).text
                    name_results_bool = bool(name_results_text)
                except:
                    name_results_bool = False
        
        if name_results_bool is True:
            results = html.find_all("div", {"data-component-type": "s-search-result", "class": "s-result-item"})

            for result in results:
                productname = result.h2.text
                producturl = "https://amazon.com" + result.h2.a["href"]

                domainsfound.append([domain, name_results_text, productname, producturl])
            
        else:
            with open("domains_not_found.txt", "a") as f:
                f.write(f"{domain}\n")

        time.sleep(random.randint(4,70))
    
    return domainsfound

fn_get_products_and_urls()

def fn_get_products_info(listdomainsfound):

    for x in listdomainsfound:

        brand_name = ""
        rating = ""
        rating_count = ""
        base_url = x[3]
        
        response = requests.get(base_url, cookies=vcookies, headers=vheaders)
        html = BeautifulSoup(response.text, "html.parser")    
        
        # brand_name
        try:
            brand = html.find("tr", {"class": "a-spacing-none"})   
            brand_name = brand.find("span", {"class": "a-size-base"}).text
        except AttributeError:
            try:
                brand = html.find("div", {"id": "productOverview_feature_div", "class": "celwidget"})
                brand_name = brand.find("table")
                brand_name = brand_name.find("tr", {"class": "po-brand"})
                brand_name = brand_name.find("td", {"class": "a-span9"}).text
            except:
                try:
                    brand = html.find("div", {"id": "nic-po-expander-section-desktop", "class": "a-section"})
                    brand_name = brand.find("div", {"id": "nic-po-expander-content"})
                    brand_name = brand_name.find("tr", {"class": "a-spacing-small po-brand"})
                    brand_name = brand_name.find("td", {"class": "a-span9"}).text
                except:
                    brand_name = None
        
        # rating
        try:
            rating = html.find("span", {"class": "a-icon-alt"}).text
        except AttributeError:
            try:
                rating = html.find("div", {"id": "averageCustomerReviews_feature_div", "class": "celwidget"})
                rating = rating.find("span", {"class": "a-declarative", "data-action": "acrStarsLink-click-metrics"})
                rating = rating.find("span", {"class": "a-icon-alt"}).text
            except:
                rating = None
        
        # rating_count
        try:
            rating_count = html.find("span", {"id": "acrCustomerReviewText", "class": "a-size-base"}).text
        except AttributeError:
            try:
                rating_count = html.find("div", {"id": "averageCustomerReviews_feature_div", "class": "celwidget"})
                rating_count = rating_count.find("span", {"class": "a-declarative", "data-action": "acrLink-click-metrics"})
                rating_count = rating_count.find("span", {"class": "a-size-base"}).text
            except:
                rating_count = None

        db = {
            "last_modified": dt.datetime.utcnow(),
            "input_domain": x[0],
            "product_name": x[2],
            "product_url": base_url,
            "brand": brand_name,
            "rating": rating,
            "rating_count": rating_count,
        }

        query_product_name = {"product_name": db["product_name"]}
        mongo = mongo_client["domains"]["amazon"].find(query_product_name)
        mongo = [x for x in mongo]

        if len(mongo) == 0:
            mongo_client["domains"]["amazon"].insert_one(db)
            print(db,"inserted.")
        
        elif len(mongo) == 1:
            # query = {"$set": {"last_modified": dt.datetime.utcnow()}}
            # mongo_client["domains"]["amazon"].update_one(mongo, query)
            # print(db,"already exist in database and update the last_modified")
            print(db,"already exist in database")
        
        else:
            mongo_client["domains"]["amazon"].delete_many(query_product_name)
            mongo_client["domains"]["amazon"].insert_one(db)
            print(db, "deleted and inserted")
        
        time.sleep(random.randint(4,70))


fn_get_products_info(domainsfound)

print("End of Operation")
