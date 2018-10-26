
# 从0.21Claudius库里获取最新的边界点以及最新的网格数据 只需要运行一次 除非库有更新
# 目的是把网格数据从库里面写入到csv文件中 后面直接读取该文件到内存中 加快处理速度
import csv
from pymongo import MongoClient


# 初始化网格数据
def init_geo_tile():
    client = MongoClient('mongodb://pmi_mapai:pmi_mapai@192.168.0.21:27017')  # mongodb连接
    db = client.Claudius  # 库名
    my_col = db.GeoTile  # 这里修改表名
    f = open('geo_tile.csv', 'w', newline='', encoding='utf-8')
    fieldnames = ['lv1Name', 'lv2Name', 'lv3Name', 'tileNames']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    results = my_col.aggregate(
        [
            {'$match': {'meters': 1000}},
            {'$project': {'_id': 0, 'lv1Name': 1, 'lv2Name': 1, 'lv3Name': 1, 'tileNames': 1}},
            {'$unwind': '$tileNames'},
        ],
        allowDiskUse=True
    )
    for i in results:
        writer.writerow(i)


# 获取最新边界点
def get_ewsn(lv1name=None, lv2name=None, lv3name=None):
    client = MongoClient('mongodb://pmi_mapai:pmi_mapai@192.168.0.21:27017')
    db = client.Claudius
    my_col = db.GeoBoundary
    result = my_col.find({"lv1Name": lv1name, "lv2Name": lv2name, "lv3Name": lv3name}, {'boundaries': 1})
    each = result.__next__()
    each = each['boundaries']
    lng = list(map(lambda item: float(item['lng']), each))  # 经度
    lat = list(map(lambda item: float(item['lat']), each))  # 纬度
    return max(lng), min(lng), min(lat), max(lat)


# 生成带有序号的网格数据
if __name__ == '__main__':
    e, w, s, n = get_ewsn()  # 调用get_ewsn函数获取最新边界点
    init_geo_tile()
    # 调用init_geo_tile在当前目录下生成geo_tile.csv 文件形如
    '''
    lv1Name,lv2Name,lv3Name,tileNames
    上海市,浦东新区,,4809-1449
    上海市,浦东新区,,4809-1441
    上海市,浦东新区,,4809-1442
    '''
    meters = 1000  # 网格边长参数 后面250网格数据弄好之后改成250即可
    width = 0.00001 * meters  # 每个格子的经度差
    height = 0.000009 * meters  # 每个格子的纬度差
    lngSize = int((e - w) / width)+2 if (e - w) % width else int((e - w) / width)+1  # 东西向网格数量
    latSize = int((n - s) / height)+2 if (n - s) % height else int((n - s) / height)+1  # 南北向网格数量
    max_geo_index = lngSize*latSize  # 网格总数
    matrix = [None]*max_geo_index  # 初始化一个数组 数组大小等于网格总数
    #  读取之前init_geo_tile在当前目录下生成的geo_tile.csv文件 把数据读入matrix数组
    f = open('geo_tile.csv', 'r', encoding='utf-8')
    f.readline()
    reader = csv.reader(f)
    for i in reader:
        index = int(i[3].split('-')[0]) * latSize + int(i[3].split('-')[1])
        matrix[index] = i
    #  把matrix数组中各元素写入当前目录geo_tile_result.csv文件中 文件形如
    '''
    2362, 新疆维吾尔自治区, 克孜勒苏柯尔克孜自治州, 阿克陶县, 0 - 2362
    6301, 新疆维吾尔自治区, 克孜勒苏柯尔克孜自治州, 阿克陶县, 1 - 2361
    6302, 新疆维吾尔自治区, 克孜勒苏柯尔克孜自治州, 阿克陶县, 1 - 2362
    '''
    f = open('geo_tile_result.csv', 'w', newline='', encoding='utf-8')
    writer = csv.writer(f)
    for index in range(len(matrix)):
        if matrix[index]:
            writer.writerow([index] + matrix[index])








