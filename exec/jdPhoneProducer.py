import json
import random
import re
import time
import traceback

import happybase
import requests
import simplejson
from fake_useragent import UserAgent
from kafka import KafkaProducer
from selenium import webdriver

# ua 不填就随机，注意一定要在有代理的情况下使用随机ua，不然同一个ip不停更换ua的行为极容易被检测！
# user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 " \
#              "Safari/537.36 "
user_agent = ""

# 代理设置
# 拼接方式为蜻蜓代理的隧道代理方式
wait_time = random.uniform(1, 5)  # 延时时间，按代理要求来，如果不使用代理一定要设5秒左右或以上！
retry_count = 50  # 连接失败/爬不到的重试次数
# 这些是隧道代理的配置，不用就空着
proxy_user = ""
proxy_password = ""
proxy_host = "dyn.horocn.com"
proxy_port = "50000"
# 这是私密代理的配置，不用就空着
proxy_pool_host = ""

proxies = False
# 集群设置，注意hbase要填thrift服务器的端口（不支持thrift2）
set_server = "localhost"
set_kafka_port = "9092"
set_hbase_thrift_port = "9090"


# 爬取京东手机列表,并生产出去
def info_producer(server=set_server, kafka_port=set_kafka_port):
    driver = webdriver.Firefox()
    url = "https://search.jd.com/search?keyword=%E6%89%8B%E6%9C%BA&wq=%E6%89%8B%E6%9C%BA&cid3=655&cid2=653&page={0}&s={1}&click=0"
    producer = KafkaProducer(bootstrap_servers=[server + ":" + kafka_port])
    temp = 0
    for i in range(1, 100):
        # 通过设置规律的参数来获取每一页
        page_num = 2 * i - 1
        s = 1 + 50 * (i - 1)
        print("======" + url.format(page_num, s) + "======")
        driver.get(url=url.format(page_num, s))
        time.sleep(2)
        # 页面默认显示30条，通过执行下面的js使页面加载当前页面的全部数据（60条）
        js = "window.scrollTo(1000000,1000000)"
        driver.execute_script(js)
        time.sleep(2)

        # 生产消息
        for d in get_phone_list(driver):
            print(d)
            temp += 1
            producer.send(topic="jd_list", key=d["phoneid"].encode('utf-8'), value=
            (d["phonename"] + "|"
             + (d["phoneurl"] if "phoneurl" in d.keys() else "https://item.jd.com/{}.html".format(d["phoneid"]))
             + "|" + d["phoneprice"] + "|" + d["phonecommentcnt"] + "|"
             + d["phoneseller"] + "|" + d["phoneproprietary"]).encode('utf-8'))

    print("total num: %d" % temp)


# # 判断title
# title = re.findall(r'<title></title>', html, flags=re.S)
# if not title or "JD.COM" in title:
#     print(html)
#     print(rowkey + "商品已不存在")
#     false_rowkey.append(rowkey)
#     exist = False
#     break
#     # elif "登录" in title:
#     #     print("IP is blocked")
#     #     false_num += 1
#     #     delete_proxy(proxy)

# 获取手机详情，需要先获取爬取到的手机序号和url
def detail_producer(
        add_mode=False,  # 附加模式，True时会跳过已有详情数据的行，False会重新爬取所有数据
        server=set_server,
        kafka_port=set_kafka_port,
        hbase_thrift_port=set_hbase_thrift_port):
    # kafka初始化
    producer = KafkaProducer(bootstrap_servers=[server + ":" + kafka_port])

    # 扫描hbase表的每一行，取出phone_url列值
    # 注意:happybase连接参数transport和protocol如此写法是因为我遇到了
    # TTransportException（type=4,message=’TSocket read 0 bytes’）错误
    # 如果没有出现此问题，删掉后两个参数，如果出现，参考以下文章解决
    # https://blog.csdn.net/qq_41685616/article/details/106136013
    con_hbase = happybase.Connection(host=server, port=int(hbase_thrift_port), transport="framed", protocol="compact")
    con_hbase.open()
    info_table = con_hbase.table("jd_phone_info")
    # 取表中的url列
    scanner = info_table.scan(columns=["list:phone_url", "detail:name"])
    total_num = 0
    false_rowkey = []
    try:
        while True:
            row = next(scanner)
            try:
                if add_mode and str(row[1][b'detail:name'], encoding="utf-8"):
                    print("\rLooking for rowkey to fill...", end="")
                    total_num += 1
                    continue
            except KeyError:
                pass

            rowkey = str(row[0], encoding="utf-8")
            url = str(row[1][b'list:phone_url'], encoding="utf-8")
            print("Searching:ID: " + rowkey + ",url:" + url + " ,total num:" + str(total_num))
            html = None
            false_num = 0
            baned_num = 0

            # 爬取
            while false_num <= retry_count:  # 这层循环用于重新获取代理
                try:
                    exist = True
                    ban = False
                    # 获取手机详细信息
                    proxy = get_proxies()

                    # 识别js脚本
                    while false_num <= retry_count:  # 这层循环用于走完所有的重定向
                        resp = requests.get(url, headers=get_header(), proxies=proxy, timeout=5)
                        html = resp.content.decode()
                        redirect = re.findall(r"<script>window.location.href='(.*?)'</script>", html, flags=re.S)
                        if redirect:
                            # print(redirect)
                            if "passport" in redirect[0]:
                                print("IP已被ban")
                                ban = True
                                baned_num += 1
                                if proxy_pool_host:
                                    delete_proxy(proxy)
                                if baned_num > 1.0 / 2 * retry_count:
                                    if proxy_pool_host:
                                        print("过多的ip被ban，这可能是由于代理池内没有足够的ip，程序休眠10分钟...")
                                        time.sleep(600)
                                    else:
                                        raise ConnectionError("ip已经被ban，请更换ip或设置代理！")
                                time.sleep(wait_time)
                                break

                            else:
                                print(redirect)
                                url = redirect[0]
                        else:
                            break

                    if ban:
                        continue
                    else:
                        break

                except requests.exceptions.RequestException as e:
                    if false_num < retry_count:
                        print("Connection failed, reconnecting...")
                        print(e)
                        false_num += 1
                        delete_proxy(proxy)
                        time.sleep(wait_time)
                    else:
                        raise e

            # if false_num > retry_count:
            #     print("Rowkey failure:" + rowkey)
            #     false_rowkey.append(rowkey)
            #     if len(false_rowkey) > retry_count:
            #         raise ConnectionError("超过" + str(retry_count) + "条消息爬取失败")
            #     else:
            #         continue

            # if not exist:
            #     print("rowkey:" + rowkey + "商品已不存在")
            #     false_rowkey.append(rowkey)
            #     continue

            # 获取detail
            pattern = r'<ul class="parameter2 p-parameter-list">(.*?)</ul>'
            detail = re.findall(pattern, html, flags=re.S)
            if len(detail) <= 0:
                print("rowkey:" + rowkey + "数据不充足！")
                false_rowkey.append(rowkey)
                continue

            # 获取产品参数
            pattern2 = r"<li.*?>(.*?)</li>"
            lst = re.findall(pattern2, detail[0])
            # 获取品牌
            pattern3 = r'<ul id="parameter-brand" class="p-parameter-list">(.*?)</ul>'
            tmphtml = re.findall(pattern3, html, flags=re.S)
            if len(tmphtml) > 0:
                pattern4 = r"<li.*?>(.*?)<a.*?>(.*?)</a>"
                t2 = re.findall(pattern4, tmphtml[0], re.S)[0]
                lst.append(t2[0] + t2[1])
            else:
                lst.append("null")
            print(lst)

            # 分拣存入字典
            lst_keys = []
            lst_values = []
            for item in lst:
                lst_keys.append(item.split("：")[0])
                lst_values.append(item.split("：")[1])
            d_phone_detail = dict(zip(lst_keys, lst_values))

            # kafka生产
            producer.send(topic="jd_detail", key=d_phone_detail["商品编号"].encode('utf-8'), value=(
                    (d_phone_detail["商品名称"] if "商品名称" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["商品毛重"] if "商品毛重" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["商品产地"] if "商品产地" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["CPU型号"] if "CPU型号" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["运行内存"] if "运行内存" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["机身存储"] if "机身存储" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["存储卡"] if "存储卡" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["摄像头数量"] if "摄像头数量" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["后摄主摄像素"] if "后摄主摄像素" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["前摄主摄像素"] if "前摄主摄像素" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["主屏幕尺寸（英寸）"] if "主屏幕尺寸（英寸）" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["分辨率"] if "分辨率" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["屏幕比例"] if "屏幕比例" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["充电器"] if "充电器" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["热点"] if "热点" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["操作系统"] if "操作系统" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["游戏配置"] if "游戏配置" in d_phone_detail.keys() else "") + "|" +
                    (d_phone_detail["品牌"] if "品牌" in d_phone_detail.keys() else "")
            ).encode('utf-8'))

            # 防反爬，进行随机延迟
            total_num += 1
            false_num = 0
            time.sleep(wait_time)

    except StopIteration:
        print("All work finished")
        print("以下rowkey已不存在所以爬取失败，建议更新列表：")
        print(false_rowkey)

    except Exception as e:
        print("Exception occurred, rowkey:" + rowkey + ",total num:" + str(total_num))
        raise e

    pass


# 获取评论，需要先获取爬取到的手机序号
def comment_producer(
        pages=50,  # 爬取页数
        add_mode=False,  # 附加模式，True时会跳过已爬好的数据，False会重新爬取所有数据
        server=set_server,
        kafka_port=set_kafka_port,
        hbase_thrift_port=set_hbase_thrift_port):
    # kafka初始化
    producer = KafkaProducer(bootstrap_servers=[server + ":" + kafka_port])

    # 扫描hbase表的每一行，取出phone_url列值
    # 注意:happybase连接参数transport和protocol如此写法是因为我遇到了
    # TTransportException（type=4,message=’TSocket read 0 bytes’）错误
    # 如果没有出现此问题，删掉后两个参数，如果出现，参考以下文章解决
    # https://blog.csdn.net/qq_41685616/article/details/106136013
    con_hbase = happybase.Connection(host=server, port=int(hbase_thrift_port), transport="framed", protocol="compact")
    con_hbase.open()
    info_table = con_hbase.table("jd_phone_info")
    comment_table = con_hbase.table("jd_phone_comment")
    # 取表中的url列
    scanner = info_table.scan(columns=["list:phone_url"])
    total_num = 0
    false_rowkey = []
    base_url = "https://club.jd.com/comment/productPageComments.action?productId={0}&score=0&sortType=5&page={" \
               "1}&pageSize=10&isShadowSku=0&fold=1"
    try:
        while True:
            row = next(scanner)
            rowkey = str(row[0], encoding="utf-8")
            com_num = 0
            page_start = 0
            fin = False
            try:
                # 进入填充模式，首先判断评论是否爬完
                if add_mode and comment_table.row(row=rowkey + "x000", columns=["comment:fin"]):
                    print("\rLooking for rowkey to fill...", end="")
                    total_num += 1
                    continue
                else:
                    # 找到没有爬取完成的rowkey后，从前往后遍历寻找没爬到的页数
                    find_page = 0
                    while comment_table.row(row=rowkey + "x" + str(find_page).zfill(2) + "0",
                                            columns=['comment:id']):
                        find_page += 1
                    page_start = find_page

            except KeyError:
                pass

            # 每个商品爬page_num页,注意一页10条评论
            for page_num in range(page_start, pages):
                url = base_url.format(rowkey, page_num)
                comment_json = None
                false_num = 0
                print("\nSearching:ID:" + rowkey + ",page " + str(page_num) + " ,total num:" + str(total_num))

                # 爬取
                while false_num <= retry_count:
                    try:
                        proxy = get_proxies()

                        resp = requests.get(url, headers=get_header(), proxies=proxy)
                        # 判断是否进的评论页
                        if resp.json()["testId"]:
                            comment_json = resp.json()["comments"]
                            if len(comment_json) > 0:
                                break
                            # 如果进了评论页但评论仍不足，说明已经没有足够的评论
                            else:
                                fin = True
                                break

                        else:
                            print("no message")
                            print(resp.content.decode())
                            false_num += 1
                            time.sleep(wait_time)

                    except requests.exceptions.RequestException as e:
                        if false_num < retry_count:
                            print("Connection failed, reconnecting...")
                            false_num += 1
                            time.sleep(wait_time)
                        else:
                            raise e

                    except simplejson.errors.JSONDecodeError as e:
                        if false_num < retry_count:
                            print("IP已被ban...")
                            print(resp.content.decode())
                            false_num += 1
                            time.sleep(wait_time)
                        else:
                            print("重试次数过多，程序休眠一段时间")
                            time.sleep(600)

                if false_num > retry_count:
                    print("Rowkey failure:" + rowkey)
                    false_rowkey.append(rowkey)
                    if len(false_rowkey) > retry_count:
                        raise ConnectionError("超过" + str(retry_count) + "条消息爬取失败，如果你没有使用代理，说明ip已被ban"
                                                                        "，如果你的代理正常，这可能说明数据库中失效商品过多，请更新列表（尝试再次爬取手机列表）")
                    else:
                        continue

                # 如果没有足够的评论了就直接设置完成标记退出
                if fin:
                    comment_table.put(row=rowkey + "x000", data={"comment:fin": "1"})
                    break

                # 遍历数据
                for c in comment_json:
                    key = rowkey + "x" + str(com_num).zfill(3)
                    print(key, end=" ")
                    # kafka生产
                    producer.send(topic="jd_comment", key=key.encode('utf-8'),
                                  value=(
                                          str(c["id"] if "id" in c.keys() else "") + "|" +
                                          str(c["guid"] if "guid" in c.keys() else "") + "|" +
                                          str(c["content"] if "content" in c.keys() else "").replace("\n", "") + "|" +
                                          str(c["creationTime"] if "creationTime" in c.keys() else "") + "|" +
                                          str(c["score"] if "score" in c.keys() else "") + "|" +
                                          str(c["plusAvailable"] if "plusAvailable" in c.keys() else "") + "|" +
                                          str(c["mobileVersion"] if "mobileVersion" in c.keys() else "") + "|" +
                                          str(c["userClient"] if "userClient" in c.keys() else "")
                                  ).encode('utf-8'))
                    com_num += 1
                time.sleep(wait_time)
            # 爬取完后，设置标记表示已爬取完成
            total_num += 1
            comment_table.put(row=rowkey + "x000", data={"comment:fin": "1"})

    except StopIteration:
        print("All work finished")
        print("以下rowkey已不存在，因此爬取失败，建议更新列表:")
        print(false_rowkey)

    except Exception as e:
        print("Exception occurred, rowkey:" + rowkey + ",total num:" + str(total_num))
        raise e


# ===========================================以下为工具方法=======================================================
def get_phone_list(driver):
    for li in driver.find_elements_by_xpath("//div[@id='J_goodsList']/ul/li"):
        # 获取手机id
        phone_id = li.get_attribute("data-sku")
        # 获取手机名称
        phone_name = li.find_elements_by_xpath(".//div[@class='p-name p-name-type-2']/a/em")[0].text
        # 去掉“京品手机”
        pattern = r"京品手机|\n"
        phone_name = re.sub(pattern, "", phone_name, flags=re.S)
        # 获取详细页地址
        phone_url = li.find_elements_by_xpath(".//div[@class='p-name p-name-type-2']/a")[0].get_attribute(
            "href")
        # 获取手机价格
        phone_price = li.find_elements_by_xpath(".//div[@class='p-price']//i")[0].text
        # 获取评论数
        phone_comment_cnt = li.find_elements_by_xpath(".//div[@class='p-commit']/strong/a")[0].text
        # 获取销售商家
        phone_seller = li.find_elements_by_xpath(".//div[@class='p-shop']/span/a")
        # 排除没有提供商家的情况
        if len(phone_seller) > 0:
            phone_seller = phone_seller[0].text
        else:
            phone_seller = ""
        # 是否自营
        tmp = li.find_elements_by_xpath(".//div[@class='p-icons']/i[1]")
        phone_proprietary = "True" if len(tmp) > 0 and tmp[0].text == "自营" else "False"
        yield {"phoneid": phone_id, "phonename": phone_name, "phoneurl": phone_url,
               "phoneprice": phone_price, "phonecommentcnt": phone_comment_cnt, "phoneseller": phone_seller,
               "phoneproprietary": phone_proprietary}


def get_proxies():
    global proxies
    false_num = 0
    while True:
        if proxy_user and proxy_password and proxy_host and proxy_port:
            if type(proxies) != bool:
                return proxies
            else:
                proxy_meta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
                    "user": proxy_user,
                    "pass": proxy_password,
                    "host": proxy_host,
                    "port": proxy_port,
                }
                proxies = {"http": proxy_meta, "https": proxy_meta}
                return proxies

        elif proxy_pool_host:
            try:
                proxy_meta = requests.get("http://{}/get/".format(proxy_pool_host)).json().get("proxy")
                proxies = {"http": proxy_meta, "https": proxy_meta}
                return proxies
            except TypeError as e:
                if false_num <= retry_count:
                    print("Warning: No proxy available,retrying...")
                    false_num += 1
                    time.sleep(10)
                    continue
                else:
                    print("Can't get any proxy, check proxy pool.")
                    raise e
        else:
            return None


def delete_proxy(proxy):
    if proxy and proxy["http"]:
        print("删除代理:" + proxy["http"])
        requests.get("http://{0}/delete/?proxy={1}".format(proxy_pool_host, proxy["http"]))


def get_header():
    global user_agent
    if user_agent:
        return {"user-agent": user_agent}
    else:
        user_agent = UserAgent().random
        return {"user-agent": user_agent}


# def self_get_proxy_num():
#     global proxy_list
#     if len(proxy_list) <= 0:
#         resp = requests.get("https://proxyapi.horocn.com/api/v2/proxies?order_id=YRLC1722822415211137&num=20&format"
#                             "=json&line_separator=win&can_repeat=yes&user_token=6d6e2ee24d6012f702a20cbc37db537f"
#                             "").json()
#         if resp["code"] == 0:
#             for proxy_data in resp["data"]:
#                 proxy_meta = proxy_data["host"] + ":" + proxy_data["port"]
#                 proxy = {"http": proxy_meta, "https": proxy_meta}
#                 proxy_list.append(proxy)
#         elif resp["code"] == 10001:
#             time.sleep(10)
#             for proxy_data in resp["data"]:
#                 proxy_meta = proxy_data["host"] + ":" + proxy_data["port"]
#                 proxy = {"http": proxy_meta, "https": proxy_meta}
#                 proxy_list.append(proxy)
#         else:
#             raise Exception("ProxyException:" + resp["data"]["tip"])
#     return random.choice(range(0, len(proxy_list)))


if __name__ == '__main__':
    # info_producer()
    # detail_producer(server="202.194.64.164", add_mode=True)

    # url = "https://item.jd.com/100013225247.html"
    # while True:
    #     try:
    #         proxy = get_proxies()
    #         resp = requests.get(url, headers=headers, proxies=proxy, timeout=10)
    #         html = resp.content.decode()
    #         # 获取detail
    #         pattern = r'<ul class="parameter2 p-parameter-list">(.*?)</ul>'
    #         detail = re.findall(pattern, html, flags=re.S)
    #         if len(detail) <= 0:
    #             print("no message")
    #             continue
    #         # 获取产品参数
    #         pattern2 = r"<li.*?>(.*?)</li>"
    #         lst = re.findall(pattern2, detail[0])
    #         # 获取品牌
    #         pattern3 = r'<ul id="parameter-brand" class="p-parameter-list">(.*?)</ul>'
    #         tmphtml = re.findall(pattern3, html, flags=re.S)
    #         if len(tmphtml) > 0:
    #             pattern4 = r"<li.*?>(.*?)<a.*?>(.*?)</a>"
    #             t2 = re.findall(pattern4, tmphtml[0], re.S)[0]
    #             lst.append(t2[0] + t2[1])
    #         else:
    #             lst.append("null")
    #         print(lst)
    #         pass
    #     except requests.exceptions.RequestException as e:
    #         print(e)

    # comment_producer(server="202.194.64.164")
    # con_hbase = happybase.Connection(host="202.194.64.164", port=9090, transport="framed", protocol="compact")
    # con_hbase.open()
    # info_table = con_hbase.table("jd_phone_infos")
    # scanner = info_table.scan(columns=[""])
    # temp = 0
    # while True:
    #     row = next(scanner)
    #     rowkey = str(row[0], encoding="utf-8")
    #     print("Searching:ID: " + rowkey + ",url:" + url)

    while True:
        func = input("输入要运行的producer\n1:爬取手机列表 2:根据列表爬取手机详情 3:根据列表爬取评论")
        if func == "1":
            info_producer()
        elif func == "2":
            if input("是否使用addition模式？不使用则从头开始爬取，使用则遍历寻找没有爬取过的rowkey，输入\"y\"来启用") == "y":
                detail_producer(add_mode=True)
            else:
                detail_producer()
        elif func == "3":
            if input("是否使用addition模式？不使用则从头开始爬取，使用则遍历寻找没有爬取过的rowkey，输入\"y\"来启用") == "y":
                comment_producer(add_mode=True)
            else:
                comment_producer()
        else:
            print("输入错误")
