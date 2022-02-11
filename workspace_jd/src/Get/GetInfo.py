from selenium import webdriver
import time
import csv
import re


# 获取手机信息
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
            phone_seller = "null"
        # 是否自营
        tmp = li.find_elements_by_xpath(".//div[@class='p-icons']/i[1]")
        phone_proprietary = True if len(tmp) > 0 and tmp[0].text == "自营" else False
        yield {"phoneid": phone_id, "phonename": phone_name, "phoneurl": phone_url,
               "phoneprice": phone_price, "phonecommentcnt": phone_comment_cnt, "phoneseller": phone_seller,
               "phoneproprietary": phone_proprietary}


if __name__ == "__main__":
    driver = webdriver.Firefox()
    url = "https://search.jd.com/search?keyword=%E6%89%8B%E6%9C%BA&wq=%E6%89%8B%E6%9C%BA&cid3=655&cid2=653&page={0}&s={1}&click=0"
    # 添加newline=""参数设置去掉空行
    with open("../../target/jd_phone_list.csv",
              "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["商品id", "商品名称", "详情链接", "价格", "评论人数", "店铺名称", "是否自营"])
        # 此处只测试抓5页，可以修改为100，抓取全部数据
        for i in range(1, 5):
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
            # 写入csv文件
            for d in get_phone_list(driver):
                print(d)
                writer.writerow(
                    [d["phoneid"], d["phonename"], d["phoneurl"], d["phoneprice"], d["phonecommentcnt"],
                     d["phoneseller"],
                     d["phoneproprietary"]])
