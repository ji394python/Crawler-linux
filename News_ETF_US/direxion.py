import os
import pandas as pd
import requests
import json
from traceback import format_exc
import log_manager as log
import time
from bs4 import BeautifulSoup
import lxml
from datetime import datetime

# d['data']['direxionSearch']['items'][0]['date'] #日期時間
# d['data']['direxionSearch']['items'][0]['excerpt'].strip() #短篇內文
# d['data']['direxionSearch']['items'][0]['slug'] #url連結 (https://www.direxion.com/press-release/)

if __name__ == '__main__':
  
  log.processLog('==============================================================================================')
  log.processLog(f'【執行ETF_US新聞爬蟲專案】 {os.path.basename(__file__)}')
    
  #計時開始
  start = time.perf_counter() 
  
  try:
    
    tail_path = json.load(open(f"path.json",'r'))
    tail_path = tail_path['NasPublic']
    
    
    url = "https://www.direxion.com/graphql"
    web_url = 'https://www.direxion.com/press-releases/'

    payload="{\"query\":\"query ListingQuery( \
      $page: Int, $postsPerPage: Int, $keyword: String, $category: [String], $ticker: [String], \
      $productCategory: [String], $type: [String], $order: DirexionSearchOrderEnum, \
      $orderBy: DirexionSearchOrderByEnum, $featured: Boolean) \
      {\\r\\n  direxionSearch(\\r\\n    page: $page\\r\\n    postsPerPage: $postsPerPage\\r\\n    \
      keyword: $keyword\\r\\n    category: $category\\r\\n    ticker: $ticker\\r\\n    productCategory: \
      $productCategory\\r\\n    type: $type\\r\\n    order: $order\\r\\n    orderBy: $orderBy\\r\\n    \
      featured: $featured\\r\\n  ) {\\r\\n    categories {\\r\\n      id\\r\\n      name\\r\\n      slug\\r\\n      \
      __typename\\r\\n    }\\r\\n    types {\\r\\n      id\\r\\n      name\\r\\n      slug\\r\\n      __typename\\r\\n    } \
      \\r\\n    productCategories {\\r\\n      id\\r\\n      name\\r\\n      slug\\r\\n      __typename\\r\\n    }\\r\\n    \
      items {\\r\\n      __typename\\r\\n      ... on Post {\\r\\n        id\\r\\n        title\\r\\n        \
      slug\\r\\n        excerpt\\r\\n        date\\r\\n        blogPostFields {\\r\\n          blogType\\r\\n          \
      __typename\\r\\n        }\\r\\n        categories {\\r\\n          nodes {\\r\\n            id\\r\\n            \
      slug\\r\\n            name\\r\\n            __typename\\r\\n          }\\r\\n          __typename\\r\\n        }\\r\\n        \
      productCategories {\\r\\n          nodes {\\r\\n            id\\r\\n            slug\\r\\n            name\\r\\n            \
      __typename\\r\\n          }\\r\\n          __typename\\r\\n        }\\r\\n        typeEducations {\\r\\n          nodes {\\r\\n            id\\r\\n            slug\\r\\n            name\\r\\n            __typename\\r\\n          }\\r\\n          __typename\\r\\n        }\\r\\n        __typename\\r\\n      }\\r\\n      ... on EducationPost {\\r\\n        id\\r\\n        title\\r\\n        slug\\r\\n        excerpt\\r\\n        date\\r\\n        educationPostFields {\\r\\n          thumbnail {\\r\\n            sourceUrl(size: THUMBNAIL)\\r\\n            __typename\\r\\n          }\\r\\n          __typename\\r\\n        }\\r\\n        categories {\\r\\n          nodes {\\r\\n            id\\r\\n            slug\\r\\n            name\\r\\n            __typename\\r\\n          }\\r\\n          __typename\\r\\n        }\\r\\n        productCategories {\\r\\n          nodes {\\r\\n            id\\r\\n            slug\\r\\n            name\\r\\n            __typename\\r\\n          }\\r\\n          __typename\\r\\n        }\\r\\n        typeEducations {\\r\\n          nodes {\\r\\n            id\\r\\n            slug\\r\\n            name\\r\\n            __typename\\r\\n          }\\r\\n          __typename\\r\\n        }\\r\\n        __typename\\r\\n      }\\r\\n      ... on PressRelease {\\r\\n        id\\r\\n        title\\r\\n        slug\\r\\n        excerpt\\r\\n        date\\r\\n        categories {\\r\\n          nodes {\\r\\n            id\\r\\n            slug\\r\\n            name\\r\\n            __typename\\r\\n          }\\r\\n          __typename\\r\\n        }\\r\\n        productCategories {\\r\\n          nodes {\\r\\n            id\\r\\n            slug\\r\\n            name\\r\\n            __typename\\r\\n          }\\r\\n          __typename\\r\\n        }\\r\\n        typeEducations {\\r\\n          nodes {\\r\\n            id\\r\\n            slug\\r\\n            name\\r\\n            __typename\\r\\n          }\\r\\n          __typename\\r\\n        }\\r\\n        __typename\\r\\n      }\\r\\n    }\\r\\n    total\\r\\n    __typename\\r\\n  }\\r\\n}\\r\\n\",\"variables\":{\"page\":1,\"category\":[],\"type\":[\"press-releases\"],\"productCategory\":[],\"order\":\"DESC\",\"orderBy\":\"DATE\",\"postsPerPage\":1000}}"
    headers = {
      'if-none-match': '052041eaf2be177b446586ae373626acd',
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    check = False
    while response.status_code != 200:
        if count > 10:
            log.processLog(f'[direxion.py] 10次連線失敗，本日未完成ETF新聞爬取')
            print(f'[direxion.py] 10次連線失敗，本日未完成ETF新聞爬取')
            check = True
            break
        count = count + 1
        print(f'[direxion.py] ETF新聞爬取第{count}次連線失敗...5秒後重試')
        log.processLog(f'[direxion.py] ETF新聞爬取第{count}次連線失敗...5秒後重試')
        time.sleep(5)
        response = requests.request("POST", url, headers=headers, data=payload)
    
    if not check:
      log.processLog(f'[direxion.py] ETF新聞第爬取完成...進入提取階段')
      print(f'[direxion.py] ETF新聞第爬取完成...進入提取階段')
      
      d = json.loads(response.text)
      # json.dump(d,open('test.json','w+'),indent=4)

      items = d['data']['direxionSearch']['items']
      Date = [ i['date'][:10] for i in items ]
      Time = [ i['date'][11:] for i in items ]
      Title = [ i['title'] for i in items ]
      Content = [ i['excerpt'].strip().replace('<p>','').replace('</p>','') for i in items ]
      URL = [ f"{web_url}{i['slug']}" for i in items ]
      Tag = [ '、'.join([ k['name'] for k in i['categories']['nodes']]).replace("'",'') for i in items ]

      df = pd.DataFrame({'Date':Date,'Time':Time,'Tag':Tag,
                            'Title':Title,'URL':URL})
      df['Issuer'] = 'direxion'
      if not os.path.exists(f'{tail_path}'):
          os.makedirs(f'{tail_path}')
          
      df.to_csv(f"{tail_path}/direxion.csv",index=False,encoding='utf-8-sig')
      log.processLog(f'[direxion.py] 輸出：{tail_path}/direxion.csv')
      print(f'[direxion.py] 輸出：{tail_path}/direxion.csv')
      
      end = time.perf_counter()
      log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
      log.processLog('==============================================================================================')
      
  except Exception as e:
          log.processLog(f'[direxion.py] 本次ETF新聞爬蟲遇到未知問題，請查看錯誤log檔')
          log.processLog(f'---------------------')
          log.errorLog(f'{format_exc()}')
          print(e)