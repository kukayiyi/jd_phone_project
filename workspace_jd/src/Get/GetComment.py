import time
import csv
import requests
import sys
import random

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
}


def get_product():
    with open("../../target/jd_phone_list.csv",
              "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=",")
        next(reader)
        for r in reader:
            yield (r[0], r[2])


if __name__ == "__main__":
    base_url = "https://club.jd.com/comment/productPageComments.action?productId={0}&score=0&sortType=5&page={1}&pageSize=10&isShadowSku=0&fold=1"
    with open("../../target/jd_comment.csv",
              "w", encoding="utf-8", newline="") as f:

        writer = csv.writer(f)
        writer.writerow(["序号", "商品id", "guid", "评论内容",
                         "评论时间", "参考id", "参考时间", "评分",
                         "用户昵称", "顾客会员等级", "是否手机", "购物使用的平台"])
        for id, url in get_product():
            for pagenum in range(0, 101):
                url = base_url.format(id, pagenum)
                print(url)
                # with open(
                #         "C:/Users/24350/IdeaProjects/e-commerce/dataimport/src/main/java/cn/inspur/spider/file/comments_urls.txt",
                #         "a", encoding="utf-8") as furls:
                #     furls.write(url)
                #     furls.write("\n")
                resp = requests.get(url, headers=headers)
                try:
                    comment_json = resp.json()["comments"]

                    if len(comment_json) <= 0: break
                    for c in comment_json:
                        writer.writerow([c["id"] if "id" in c.keys() else "null",
                                         id,
                                         c["guid"] if "guid" in c.keys() else "null",
                                         (c["content"] if "content" in c.keys() else "null").replace("\n", ""),
                                         c["creationTime"] if "creationTime" in c.keys() else "null",
                                         c["referenceId"] if "referenceId" in c.keys() else "null",
                                         c["referenceTime"] if "referenceTime" in c.keys() else "null",
                                         c["score"] if "score" in c.keys() else "null",
                                         c["nickname"] if "nickname" in c.keys() else "null",
                                         c["plusAvailable"] if "plusAvailable" in c.keys() else "null",
                                         c["mobileVersion"] if "mobileVersion" in c.keys() else "null",
                                         c["userClient"] if "userClient" in c.keys() else "null",
                                         ])
                    sleep_second = random.uniform(1, 5)
                    print("延迟%f秒" % sleep_second)
                    time.sleep(sleep_second)
                except Exception as e:
                    print(e)
                    print("run url is :::::::", url)
                    sys.exit()
