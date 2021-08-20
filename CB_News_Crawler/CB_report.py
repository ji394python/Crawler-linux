import pandas as pd 
import json

if __name__ == '__main__':
    rootPath = json.load(open('set.json','r+'))['output_dir_path']
    output_dir_path = f'{rootPath}/News_CB'
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
