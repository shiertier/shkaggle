import os,time,json

def mkdir(path):
    os.system(f"mkdir -p {path}")

def environ(name,value):
    os.environ[name] = value

def kaggle_login(username,key):
    os.environ["KAGGLE_USERNAME"] = username
    os.environ['KAGGLE_KEY'] = key

def kaggle_data_create(username,title,directory):
    make_dataset_metadata(username,title,directory)
    os.system(f"kaggle datasets create -p {directory}")

def kaggle_data_download(username,title,directory):
    os.system(f"kaggle datasets download {username}/{title} -p {directory} --unzip")

def kaggle_data_upload(username,title,directory):
    make_dataset_metadata(username,title,directory)
    os.system(f"kaggle datasets version -p {directory} -m 'Updated data'")

def cp(a,b):
    os.system(f"cp -r {a} {b}")

def check_folder_size(directory, min_size):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)
    return total_size >= min_size

def make_sure_download(username,title,directory,size=1024):
    kaggle_data_download(username,title,directory)
    while not check_folder_size(directory, size):
        time.sleep(5)
        print("db下载失败，等待5s")
        kaggle_data_download(username,title,directory)


def make_dataset_metadata(username,title,directory):
    os.system(f"kaggle datasets init -p {directory}")
    metadata_file = os.path.join(directory, 'dataset-metadata.json')
    with open(metadata_file, 'r') as file:
        json_data = json.load(file)
    json_data['title'] = title
    json_data['id'] = f"{username}/{title}"
    updated_json = json.dumps(json_data, indent=2)
    with open(metadata_file, 'w') as file:
        file.write(updated_json)

def make_kernel_metadata(username, title, directory, py_name):
    data = {
        "id": f"{username}/{title}",
        "title": title,
        "code_file": py_name,
        "language": "python",
        "kernel_type": "script",
        "is_private": "true",
        "enable_gpu": "false",
        "enable_tpu": "false",
        "enable_internet": "true",
        "dataset_sources": [],
        "competition_sources": [],
        "kernel_sources": [],
        "model_sources": []
    }

    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(f'{directory}/kernel-metadata.json', 'w') as f:
        json.dump(data, f, indent=1)

def kaggle_kernels_push(username,title,directory,py_name):
    make_kernel_metadata(username,title,directory,py_name)
    os.system(f"kaggle kernels push -p {directory}")
