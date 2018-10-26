import re
import sys
from pymongo import MongoClient
from datetime import datetime


def convert(lng, lat):
    MCBAND = [1.289059486E7, 8362377.87, 5591021, 3481989.83, 1678043.12, 0]
    MC2LL = [[1.410526172116255E-8, 8.98305509648872E-6, -1.9939833816331, 200.9824383106796, -187.2403703815547,
              91.6087516669843, -23.38765649603339, 2.57121317296198, -0.03801003308653, 1.73379812E7],
             [-7.435856389565537E-9, 8.983055097726239E-6, -0.78625201886289, 96.32687599759846, -1.85204757529826,
              -59.36935905485877, 47.40033549296737, -16.50741931063887, 2.28786674699375, 1.026014486E7],
             [-3.030883460898826E-8, 8.98305509983578E-6, 0.30071316287616, 59.74293618442277, 7.357984074871,
              -25.38371002664745, 13.45380521110908, -3.29883767235584, 0.32710905363475, 6856817.37],
             [-1.981981304930552E-8, 8.983055099779535E-6, 0.03278182852591, 40.31678527705744, 0.65659298677277,
              -4.44255534477492, 0.85341911805263, 0.12923347998204, -0.04625736007561, 4482777.06],
             [3.09191371068437E-9, 8.983055096812155E-6, 6.995724062E-5, 23.10934304144901, -2.3663490511E-4,
              -0.6321817810242, -0.00663494467273, 0.03430082397953, -0.00466043876332, 2555164.4],
             [2.890871144776878E-9, 8.983055095805407E-6, -3.068298E-8, 7.47137025468032, -3.53937994E-6,
              -0.02145144861037, -1.234426596E-5, 1.0322952773E-4, -3.23890364E-6, 826088.5]]
    for i in range(len(MCBAND)):
        if abs(lat) >= MCBAND[i]:
            factor = MC2LL[i]
            break
    x = factor[0] + factor[1] * abs(lng)
    temp = abs(lat) / factor[9]
    y = factor[2] + factor[3] * temp + factor[4] * temp * temp + factor[5] * temp * temp * temp + factor[6] * temp * temp * temp * temp + factor[7] * temp * temp * temp * temp * temp + factor[8] * temp * temp * temp * temp * temp * temp
    if lng < 0: x = x * -1
    if lat < 0: y = y * -1
    return x, y


def baidu_convert():
    #  mongodb连接信息
    client = MongoClient('mongodb://pmi_data:pmi_data@192.168.0.11:27017')  # mongodb连接
    db = client.DianpingWaimaiALL  # 待处理数据所在库
    my_col = db.BaiduShop201809_orig  # 待处理数据所在表
    results = my_col.find()
    for doc in results:
        lng, lat = convert(float(doc['shopLng']), float(doc['shopLat']))
        doc['geoPoint'] = {}
        doc['geoPoint']['lon'] = lng
        doc['geoPoint']['lat'] = lat
        my_col.replace_one({'_id': doc['_id']}, doc)


start_time = datetime.now()
# mongodb连接信息
client = MongoClient('mongodb://pmi_data:pmi_data@192.168.0.11:27017')  # mongodb连接
db = client.Octavius  # 待处理数据所在库
col_name = 'WaimaiBaidu_201808_Raw'
col_name_new = col_name + '_Adjusted'
my_col = db.get_collection(col_name)
if re.search(r'[Bb]aidu', col_name):
    # 定义一个对象存放字段检误结果
    fields_count = {
        'shopId': 0,
        'shopName': 0,
        'city': 0,
        'address': 0,
        'geoPoint': 0,
        'telephones': 0
    }
    # 定义一个对象 存放检误字段的样本
    fields_type = {
        'shopId': '',  # 店铺id
        'shopName': '',  # 店铺名称
        'city': '',  # 城市
        'address': '',  # 地址
        'geoPoint': {},  # 经纬度
        'telephones': []  # 电话
    }
elif re.search(r'[Ee]leme', col_name):
    fields_count = {
        'shopId': 0,
        'shopName': 0,
        'city': 0,
        'address': 0,
        'geoPoint': 0,
        'telephones': 0
    }
    fields_type = {
        'shopId': '',  # 店铺id
        'shopName': '',  # 店铺名称
        'city': '',  # 城市
        'address': '',  # 地址
        'geoPoint': {},  # 经纬度
        'telephones': []  # 电话
    }
elif re.search(r'[Mm]eituan', col_name):
    fields_count = {
        'shopId': 0,
        'shopName': 0,
        'city': 0,
        'address': 0,
        'geoPoint': 0,
        'telephones': 0,
        'avgPrice': 0
    }
    fields_type = {
        'shopId': '',  # 店铺id
        'shopName': '',  # 店铺名称
        'city': '',  # 城市
        'address': '',  # 地址
        'geoPoint': {},  # 经纬度
        'telephones': [],  # 电话
        'avgPrice': 0.0  # 人均
    }
elif re.search(r'[Dd]ian[Pp]ing', col_name):
    fields_count = {
        'shopId': 0,
        'shopName': 0,
        'city': 0,
        'address': 0,
        'geoPoint': 0,
        'telephones': 0,
        'avgPrice': 0,
        'IsShutdown': 0,
        'bigCate': 0
    }
    fields_type = {
        'shopId': '',  # 店铺id
        'shopName': '',  # 店铺名称
        'city': '',  # 城市
        'address': '',  # 地址
        'geoPoint': {},  # 经纬度
        'telephones': [],  # 电话
        'avgPrice': 0.0,  # 人均
        'IsShutdown': True,  # 是否闭店
        'bigCate': ''  # 类别
    }
else:
    print('待处理的表跟四大平台无关')
    sys.exit()

if 'status' not in my_col.find().limit(1).__next__().keys():
    print(1)
    my_col.update_many({}, {'$set': {'status': 0}})

# 定义需要重命名的字段 这里是各平台通用的 可以多不能少
fields_rename = {
    'id': 'shopId',
    'title': 'shopName',
    'shopAddress': 'address',
    'shopPhone': 'telephones',
    'shopLat': 'geoPoint.lat',  # 针对饿了么的 不过也不影响其他
    'shopLng': 'geoPoint.lon',  # 针对饿了么的 不过也不影响其他
    'catName1': 'bigCate'  # 针对比特太空点评数据的
}
# 遍历重命名字段 要把A重命名为B A如果不存在，什么也不做，A存在B不存在，把A重命名为B，A存在B也存在，什么也不做
for k, v in fields_rename.items():
    my_col.update_many({v: {'$exists': False}}, {'$rename': {k: v}})

# 遍历检误字段 查找存在A字段且A长度为0（涵盖了telephones为空数组，以及各种值为''的记录）的记录，A字段值为true的记录（点评开闭点状态为true的），不存在A字段的记录
total = my_col.count()
for k, v in fields_type.items():
    condition = [
        {k: {'$size': 0}}, {k: None}, {k: True}
    ]
    fields_count[k] = my_col.find({'$or': condition}).count() / total

# 把检核结果组成一个对象存放在当前数据库里一个叫check_result的集合里
result_item = fields_count
result_item['source'] = re.search(r'(.*?)_', col_name).group(1)
result_item['month'] = re.search(r'\d+', col_name).group()
result_item['is_new'] = 1  # 该检核结果是否为最新 1为最新0为之前的
result_item['count'] = total
end_time = datetime.now()  # 结束时间等于语句运行到此的时间（格林尼治时间)
result_item['start_time'] = start_time
result_item['process_time'] = (end_time-start_time).seconds
db.check_result.update_many({'source': result_item['source'], 'month': result_item['month']}, {'$set': {'is_new': 0}})
db.check_result.insert_one(result_item)  # 插入当前检误结果
my_col.rename(col_name_new)  # Raw表重命名为Raw_Adjusted,这里没有设置dropTarget为true，如果目标表已存在，则重命名操作失败。
