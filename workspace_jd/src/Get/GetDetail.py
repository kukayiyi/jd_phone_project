import time
import csv
import re
import requests
import random


def get_product():
    with open("../../target/jd_phone_list.csv",
              "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=",")
        next(reader)
        for r in reader:
            yield (r[0], r[2])


headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
}


def get_product_html(url):
    resp = requests.get(url, headers=headers)
    html = resp.content.decode()
    return html


def get_product_detail(html):
    # 获取产品参数html
    pattern = r'<ul class="parameter2 p-parameter-list">(.*?)</ul>'
    detail = re.findall(pattern, html, flags=re.S)
    lst2 = []
    if len(detail) > 0:
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
            lst.append("品牌：null")
        print(lst)
        return lst
    else:
        return lst2


if __name__ == "__main__":
    # 设置newline=""表示去掉写入空行
    with open(
            "../../target/jd_phone_detail.csv",
            "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["商品名称", "商品编号", "商品毛重", "商品产地", "CPU型号", "运行内存", "机身存储", "存储卡", "摄像头数量", "后摄主摄像素", "前摄主摄像素", "主屏幕尺寸（英寸）",
             "分辨率", "屏幕比例", "充电器", "热点", "操作系统", "游戏配置", "品牌"])
        num = 0
        current = 2082
        num2 = 0
        for id, url in get_product():
            print(id, url)
            html = get_product_html(url)
            # 获取手机详细参数信息
            lst = get_product_detail(html)
            if len(lst) <= 0:
                continue
            # 将参数信息转换为dict
            lst_keys = [item.split("：")[0] for item in lst]
            # 让表头只写一次
            if num == 0:
                writer.writerow(lst_keys)
                num += 1
            lst_values = [item.split("：")[1] for item in lst]
            d_phone_detail = dict(zip(lst_keys, lst_values))
            writer.writerow([
                d_phone_detail["商品名称"] if "商品名称" in d_phone_detail.keys() else "null",
                d_phone_detail["商品编号"] if "商品编号" in d_phone_detail.keys() else "null",
                d_phone_detail["商品毛重"] if "商品毛重" in d_phone_detail.keys() else "null",
                d_phone_detail["商品产地"] if "商品产地" in d_phone_detail.keys() else "null",
                d_phone_detail["CPU型号"] if "CPU型号" in d_phone_detail.keys() else "null",
                d_phone_detail["运行内存"] if "运行内存" in d_phone_detail.keys() else "null",
                d_phone_detail["机身存储"] if "机身存储" in d_phone_detail.keys() else "null",
                d_phone_detail["存储卡"] if "存储卡" in d_phone_detail.keys() else "null",
                d_phone_detail["摄像头数量"] if "摄像头数量" in d_phone_detail.keys() else "null",
                d_phone_detail["后摄主摄像素"] if "后摄主摄像素" in d_phone_detail.keys() else "null",
                d_phone_detail["前摄主摄像素"] if "前摄主摄像素" in d_phone_detail.keys() else "null",
                d_phone_detail["主屏幕尺寸（英寸）"] if "主屏幕尺寸（英寸）" in d_phone_detail.keys() else "null",
                d_phone_detail["分辨率"] if "分辨率" in d_phone_detail.keys() else "null",
                d_phone_detail["屏幕比例"] if "屏幕比例" in d_phone_detail.keys() else "null",
                d_phone_detail["充电器"] if "充电器" in d_phone_detail.keys() else "null",
                d_phone_detail["热点"] if "热点" in d_phone_detail.keys() else "null",
                d_phone_detail["操作系统"] if "操作系统" in d_phone_detail.keys() else "null",
                d_phone_detail["游戏配置"] if "游戏配置" in d_phone_detail.keys() else "null",
                d_phone_detail["品牌"] if "品牌" in d_phone_detail.keys() else "null",
            ])
            sleep_second = random.uniform(1, 5)
            print("延迟%f秒" % sleep_second)
            time.sleep(sleep_second)