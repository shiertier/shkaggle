import os
import time
import random
import string
import json
import pandas as pd

def get_status_counts(table_name):
    """
    统计给定表格中不同状态(status)的数量及其所占比例。

    参数:
    - table_name: 一个表格对象，需要有一个名为'status'的列用于统计状态。

    返回值:
    - status_counts: 一个DataFrame，包含三列：
        1. 'status': 状态值。
        2. 'count': 每个状态值出现的次数。
        3. 'percentage': 每个状态值所占的百分比。
    """

    statuses = table_name.select(table_name.status)
    status_counts = pd.DataFrame(statuses.dicts()).groupby('status').size().reset_index(name='count')
    status_counts['percentage'] = (status_counts['count'] / status_counts['count'].sum()) * 100

    return status_counts

def generate_random_string(length=10):
    """
    生成一个长度为length的随机字符串，包含字母和数字。
    """
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

def remove_other(exception,path):
    """
    删除指定目录下不以给定扩展名结尾的所有文件。

    参数:
    - exception: 要保留的文件的扩展名（例如：'.txt'）。
    - path: 目标目录的路径。
    """
    for filename in os.listdir(path):
        if not filename.endswith(exception):
            file_path = os.path.join(path, filename)
            os.remove(file_path)

def create_time():
    """
    获取当前时间，并以"小时时分钟分"的格式返回。
    """
    import datetime
    current_time = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    time_str = current_time.strftime("%H时%M分")
    return time_str

def create_file(path,name):
    file_path = os.path.join(path, name)
    open(file_path, 'x').close()

def create_and_write_file(path, name, df):
    file_path = os.path.join(path, f"{name}.csv")
    df.to_csv(file_path, index=False)
    

from peewee import *
from requests.exceptions import SSLError

def get_db(db_file):
    db = SqliteDatabase(db_file)

    class BaseModel(Model):
        class Meta:
            database = db

    class Main(BaseModel):
        key = PrimaryKeyField()
        id = IntegerField(unique=True)
        username = TextField(unique=True)
        country = TextField(null=True)
        followers_count = IntegerField(null=True)
        full_name = TextField(null=True)
        skills = TextField(null=True)
        software_items = TextField(null=True)
        projects_count = IntegerField(null=True)
        status = IntegerField(null=True)
        class Meta:
            table_name = 'main'

    class Main2(Main):
        class Meta:
            table_name = 'main2'

    class Items(BaseModel):
        key = AutoField(primary_key=True)
        progress_id = IntegerField(null=True)
        user_id = IntegerField(null=True)
        main = CharField(unique=True)
        date = CharField(max_length=8)
        title = TextField(null=True)
        description = TextField(null=True)
        tags = TextField(null=True)
        categories = TextField(null=True)
        mediums = TextField(null=True)
        software = TextField(null=True)
        adult = IntegerField(null=True)
        views = IntegerField(null=True)
        likes = IntegerField(null=True)
        comments = IntegerField(null=True)
        status = IntegerField(null=True)
        class Meta:
            table_name = 'items'

    class pics(BaseModel):
        pic_id = IntegerField(null=True)
        username = TextField(null=True)
        title = TextField(null=True)
        position = IntegerField(null=True)
        width = IntegerField(null=True)
        height = IntegerField(null=True)
        url = TextField(null=True)
        progress_id = IntegerField(null=True)
        user_id = IntegerField(null=True)
        status = IntegerField(null=True)
        class Meta:
            table_name = 'pics'
    return db,Items,pics

import json, cloudscraper, os,time

def get_progress_json(progress_main="timwarnock",max_retries=3):
    if "http" in progress_main:
        return False,False  # 如果存在，则直接返回False
    os.makedirs("json", exist_ok=True)
    terminate_flag = False
    sucess = True
    for attempt in range(max_retries):
        try:
            #url = f'https://{username}.artstation.com/rss?page={page}'
            #url = f"https://www.artstation.com/users/{username}.json"
            url = f"https://www.artstation.com/projects/{progress_main}.json"
            ignoreCertificateError = False
            if  ignoreCertificateError:
                import ssl
                context = ssl._create_unverified_context()
                scraper = cloudscraper.create_scraper(ssl_context=context)
            else:
                scraper = cloudscraper.create_scraper()
            response = scraper.get(url)
            if response.status_code == 200:
                data = response.json()
                os.makedirs("json", exist_ok=True)
                with open(f"json/{progress_main}.json", 'w') as file:
                    json.dump(data, file, indent=2)
            elif response.status_code == 429:
                print("请求失败，状态码：" + str(response.status_code))
                terminate_flag = True
                sucess = False
                return sucess, terminate_flag
            elif response.status_code == 403:
                terminate_flag = True
                sucess = False
                return sucess, terminate_flag
            elif response.status_code == 404:
                sucess = False
            elif response.status_code == 502:
                print("请求失败，状态码：" + str(response.status_code))
                import random
                def random_sleep():
                    sleep_time = random.uniform(0.1, 1.5)  # 生成0.1到1.5之间的随机浮点数
                    time.sleep(sleep_time)  # 睡眠指定的时间间隔
                random_sleep()
            else:
                print("请求失败，状态码：" + str(response.status_code))
                terminate_flag = True
        except SSLError as e:
            print(f"An SSL error occurred: {e}")
            if attempt < max_retries - 1:
                attempt += 1
                ignoreCertificateError = False
                time.sleep(0.2)  # 等待一段时间后重试
            else:
                print("Max retries exceeded for SSL error.")
                sucess = False
    if terminate_flag:
        sucess = False
    return sucess, terminate_flag

def progress_all(progress_main = "L2xmDv"):
    json_path = f"json/{progress_main}.json"
    with open(json_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    tags = '; '.join(data["tags"])
    categories = '; '.join([category["name"] for category in data["categories"]])
    mediums = '; '.join([medium["name"] for medium in data["mediums"]])
    software_items = '; '.join([item["name"] for item in data["software_items"]])
    hide_as_adult = 1 if data['hide_as_adult'] else 0

    # 获取 views_count、likes_count 和 comments_count 字段的值
    views_count = data['views_count']
    likes_count = data['likes_count']
    comments_count = data['comments_count']

    progress_id = data['id']

    data_dict = {
        'progress_id': progress_id,
        "tags": tags,
        "categories": categories,
        "mediums": mediums,
        "software": software_items,
        "adult": hide_as_adult,
        "views": views_count,
        "likes": likes_count,
        "comments": comments_count,
        "status": 1
    }
    #print(data_dict)

    result = []
    username = data['user']['full_name']
    title = data['title']
    user_id = data['user']['id']
    for asset in data['assets']:
        if asset['has_image'] and asset['asset_type'] == 'image':
            result.append({
                'pic_id': asset['id'],
                'username': username,
                'title': title,
                'position': asset['position'],
                'width': asset['width'],
                'height': asset['height'],
                'url': asset['image_url'],
                'user_id': user_id,
                'progress_id': progress_id,
                'status':0
            })
    #for item in result:
    #    print(item)
    os.remove(json_path)
    return data_dict,result

def continuous_clear_error_projects(table_name):
    main_records = table_name.select().order_by(table_name.key.asc())
    count = 0
    some_set = set()
    for record in main_records:
        if record.status == 999:
            count += 1
            if count >= 5:
                for i in range(record.key-count+1, record.key+1):
                    some_set.add(i)
        else :
            count = 0
    table_name.update(status=0).where(table_name.key.in_(some_set)).execute()

def b(db_file,batch_size):
    db,Items,pics = get_db(db_file)
    #continuous_clear_error_projects(Items)
    datas = (Items.select(Items.main,Items.key).where(Items.status == 0).limit(2048))

    if datas.exists():
        data_list = [(data.main, data.key) for data in datas.execute()]
        import random
        random.shuffle(data_list)
    else:
        return 0
    
    import threading
    import queue
    
    def process_user(progress_main, result_queue):
        sucess, terminate_flag = get_progress_json(progress_main)
        result_queue.put((progress_main, sucess, terminate_flag))

    def process_batch(batch_data):
        result_queue = queue.Queue()  # 为每个批次创建一个新的队列
        threads = []
        
        # 为批次中的每个数据项创建一个线程
        for progress_main ,key in batch_data:
            print(f"正在进行 {progress_main}, {key}")
            thread = threading.Thread(target=process_user, args=(progress_main, result_queue))
            thread.start()
            threads.append(thread)
        # 等待所有线程结束
        for thread in threads:
            thread.join()
        # 从队列中取出所有结果并转换为列表
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())
        
        return results

    #sucess, terminate_flag = get_progress_json(progress_main)
    # 定义 batch 函数，将数据分成多个批次
    def batch(iterable,batch_size):
        iterable = list(iterable)  # 将可迭代对象转换成列表
        random.shuffle(iterable)  # 将列表元素随机排序
        
        batch_sizes = list(range(1, batch_size+1))  # 批量大小范围为1到16
        current_index = 0  # 当前索引位置
        
        while current_index < len(iterable):
            batch_size = random.choice(batch_sizes)  # 随机选择一个批量大小
            yield iterable[current_index:current_index+batch_size]  # 生成当前批次的元素子集
            current_index += batch_size  # 更新当前索引位置
    db.create_tables([pics])
    batches = batch(data_list, batch_size)
    num = 0
    status_counts = get_status_counts(Items)
    print(status_counts)
    for batch_data in batches:
        for progress_main, sucess, terminate_flag  in process_batch(batch_data):
            if sucess:
                try:
                    data_dict,result = progress_all(progress_main)
                    Items.update(data_dict).where(Items.main == progress_main).execute()
                    #db.create_tables([pics])
                    try:
                        with db.atomic():
                            for item in result:
                                pics.create(**item)
                        Items.update(status=2).where(Items.main == progress_main).execute()
                        num += 1
                        print(num)
                    except Exception as e:
                        # 前一个操作失败抛出异常
                        print(f"操作失败：{e}")

                except Exception as e:
                    Items.update(status=999).where(Items.main == progress_main).execute()
            else:
                if terminate_flag and num == 0:
                    return 2
                elif terminate_flag:
                    return 1
                else:
                    Items.update(status=999).where(Items.main == progress_main).execute()

    return 1
