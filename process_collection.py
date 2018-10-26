
import csv
import sys
from bson import ObjectId
from pymongo import MongoClient
from save_to_csv import get_ewsn


# 根据经纬度生成网格编号 调用时进行异常值检查
def get_tile_name(lng, lat):
    i = int((lng-w)/(0.00001 * meters))
    j = int((lat-s)/(0.000009 * meters))
    return i, j


def main():
    # 定义一个dictionary 存放省份对应的表名
    provinces = {
        '安徽省': 'Anhui',
        '北京市': 'Beijing',
        '重庆市': 'Chongqing',
        '福建省': 'Fujian',
        '甘肃省': 'Gansu',
        '广东省': 'Guangdong',
        '广西壮族自治区': 'Guangxi',
        '贵州省': 'Guizhou',
        '海南省': 'Hainan',
        '河北省': 'Hebei',
        '黑龙江省': 'Heilongjiang',
        '河南省': 'Henan',
        '香港特别行政区': 'Hongkong',
        '湖北省': 'Hubei',
        '湖南省': 'Hunan',
        '江苏省': 'Jiangsu',
        '江西省': 'Jiangxi',
        '吉林省': 'Jilin',
        '辽宁省': 'Liaoning',
        '澳门特别行政区': 'Macau',
        '内蒙古自治区': 'NeiMongol',
        '宁夏回族自治区': 'Ningxia',
        '青海省': 'Qinghai',
        '陕西省': 'Shaanxi',
        '山东省': 'Shandong',
        '上海市': 'Shanghai',
        '山西省': 'Shanxi',
        '四川省': 'Sichuan',
        '天津市': 'Tianjin',
        '新疆维吾尔自治区': 'Xinjiang',
        '西藏自治区': 'Xizang',
        '云南省': 'Yunnan',
        '浙江省': 'Zhejiang',
    }
    insert_by_province = {}
    tables = []
    names = locals()

    # 根据provinces动态生成多个list及lambda表达式 每个省份对应一个list及lambda表达式
    for key, value in provinces.items():
        names[value] = []
        insert_by_province[key] = lambda item: names[provinces[key]].append(item)

    # 初始化一些变量
    width = 0.00001 * meters  # 每个格子的经度差
    height = 0.000009 * meters  # 每个格子的纬度差
    lngSize = int((e - w) / width) + 2 if (e - w) % width else int((e - w) / width) + 1  # 东西向网格数量
    latSize = int((n - s) / height) + 2 if (n - s) % height else int((n - s) / height) + 1  # 南北向网格数量
    max_geo_index = lngSize * latSize  # 网格总数
    matrix = [None] * max_geo_index  # 初始化一个数组 数组大小等于网格总数

    # 读取save_to_csv生成的geo_tile_result.csv到内存matrix list中 网格编号和list下标一一对应
    f = open('geo_tile_result.csv', 'r', encoding='utf-8')
    reader = csv.reader(f)
    error_f = open('error.csv', 'w', newline='', encoding='utf-8')
    error_writer = csv.writer(error_f)
    for i in reader:
        index = int(i[0])
        matrix[index] = i

    #  遍历my_col集合中的数据 根据各记录经纬度生成网格编号 根据网格编号直接获取matrix中省市区县
    results = my_col.find()
    batch_num = 0
    for doc in results:
        try:
            lng = float(doc['geoPoint']['lon'])
            lat = float(doc['geoPoint']['lat'])
        except:
            doc['status'] = 2  # 状态码 2 表示该条记录没有经纬度字段或者经纬度字段不能转换为浮点数
            error_writer.writerow([doc['_id'], 2])
            continue
        i, j = get_tile_name(lng, lat)
        if i >= lngSize or j >= latSize or i < 0 or j < 0:
            doc['status'] = 3  # 状态码 3 表示返回的网格编号超过最大网格长度或者宽度
            error_writer.writerow([doc['_id'], 3])
            continue
        if not matrix[i * latSize + j]:
            doc['status'] = 4  # 状态码 4 表示返回的网格编号在库里找不到对应值
            error_writer.writerow([doc['_id'], 4])
            continue
        doc['status'] = 0  # 状态码 0 表示数据正常
        doc['lv1Name'] = matrix[i * latSize + j][1]  # 省
        doc['lv2Name'] = matrix[i * latSize + j][2]  # 市
        doc['lv3Name'] = matrix[i * latSize + j][3]  # 区县
        doc['tileNames2'] = {str(meters): matrix[i * latSize + j][4]}  # 网格编号
        # 根据该条记录的lv1Name来决定把该记录插入到哪个list
        key = doc['lv1Name']
        insert_by_province[key](doc)
        batch_num += 1  # 插入条数计数值加一
        if batch_num == 100000:  # 这里设置批处理条数 太大内存吃不消 太小频繁IO影响效率
            for key, value in provinces.items():  # 遍历之前定义的各省份list
                if names[value]:  # 如果该省份对应的list不为空
                    collection = value
                    db_target.get_collection(collection).insert_many(names[value])  # 批量插入
                    names[value].clear()  # 把该省份数据对应的数组清空 限制内存消耗
            batch_num = 0
    # 把最后不足一批的数据入库
    if batch_num != 100000:
        for key, value in provinces.items():
            if names[value]:
                db_target.get_collection(value).insert_many(names[value])
                names[value].clear()

    # 把error.csv中各记录按id批量更新错误码到原集合（避免读取的时候一条条insert 提高效率）
    error_f = open('error.csv', 'r', encoding='utf-8')
    error_reader = csv.reader(error_f)
    error_2, error_3, error_4 = [], [], []
    for line in error_reader:
        if line[1] == '2':
            error_2.append(ObjectId(line[0]))
        elif line[1] == '3':
            error_3.append(ObjectId(line[0]))
        elif line[1] == '4':
            error_4.append(ObjectId(line[0]))
    my_col.update_many({'_id': {'$in': error_2}}, {'$set': {'status': 2}})
    my_col.update_many({'_id': {'$in': error_3}}, {'$set': {'status': 3}})
    my_col.update_many({'_id': {'$in': error_4}}, {'$set': {'status': 4}})


if __name__ == '__main__':
    #  mongodb连接信息
    client = MongoClient('mongodb://pmi_data:pmi_data@192.168.0.11:27017')  # mongodb连接
    db = client.Octavius  # 待处理数据所在库
    my_col = db.Eleme_201804_Raw_Adjusted  # 待处理数据所在表
    db_target = client.test  # 目标库名 按省份拆分好的集合会生成在这个库里
    e, w, s, n = get_ewsn()  # e w s n经度最大值（东） 经度最小值（西） 纬度最小值（南） 纬度最大值（北）
    meters = 1000  # 网格边长参数 后面250网格数据弄好之后改成250即可

    if db_target.list_collection_names():
        print('目标库已存在，如果确定要执行，请手动删除目标库')
        sys.exit()
    main()









