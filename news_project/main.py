import os
import csv
import time
import os.path
from os import path
from datetime import datetime
import pandas as pd
from pandas_datareader import data as pdr 

reason_dict = {'11a': '增資', '11b': '減資', '11c': '其他利益', '14a': '除權', '14b': '除息', '14c': '除權息', '17': '股東常會', 
				'32': '股東常會', '51': '金控/股份轉換', '99': '其他1'}

stock_name_pd = pd.read_csv('stock_index_confirm.csv')['name']
stock_name_list = []
for i in stock_name_pd:
	stock_name_list.append(i)

holiday_pd = pd.read_csv('holiday.csv')['date']
holiday_list = []
for i in holiday_pd:
	cache_list = i.split('/')
	year = cache_list[0]
	month = cache_list[1]
	day = cache_list[2]
	if len(month) == 1:
		month = '0' + month
	if len(day) == 1:
		day = '0' + day
	holiday_list.append(year + '/' + month + '/' + day)

def finding(folder_path):
	allFileList = os.listdir(folder_path)
	total_print_list = pd.DataFrame()
	return_list = []

	for i in allFileList:
		nameoffile = i
		file = open(folder_path + '/' + i, 'r', encoding = 'utf-8')
		words = str(file.read())
		#nameofstock[0] = 代號, nameofstock[1] = 中文名, nameofstock[2] = 發言時間
		nameofstock = i.split('_')

		try:
			if nameofstock[1] in stock_name_list:
				if words.find('停止過戶起始日期') != -1:
					#日期
					pos_trade_date = words.find('停止過戶起始日期')
					date_string = words[pos_trade_date:pos_trade_date + 19].strip()
					date_string = date_string.split(':')
					cache_date_string = date_string[1].split('/')
					if len(cache_date_string) == 1:
						cache_date_string1 = date_string[1].split('年')
						cache_date_string2 = cache_date_string1[1].split('月')
						cache_date_string3 = cache_date_string2[1].split('日')
						cache_date_string = ["".join(filter(str.isdigit, cache_date_string1[0])), cache_date_string2[0], cache_date_string3[0]]
					year = str(int(cache_date_string[0]) + 1911)
					cache_date_list = year + '/' + cache_date_string[1] + '/' + cache_date_string[2]
					time_stamp = int(time.mktime(time.strptime(cache_date_list, '%Y/%m/%d')))
					count = 0
					#停券起日
					while count < 6:
						time_stamp -= 86400
						cache = time.strftime('%Y/%m/%d', time.localtime(time_stamp))
						if str(cache) in holiday_list:
							continue
						elif datetime.strptime(cache, '%Y/%m/%d').weekday() == 5:
							continue
						elif datetime.strptime(cache, '%Y/%m/%d').weekday() == 6:
							continue
						else:
							count += 1
					cache_list = cache.split('/')
					start_date = str(int(cache_list[0]) - 1911) + '/' + cache_list[1] + '/' + cache_list[2]
					#停券迄日
					count = 0
					while count < 3:
						time_stamp += 86400
						cache = time.strftime('%Y/%m/%d', time.localtime(time_stamp))
						if str(cache) in holiday_list:
							pass
						elif datetime.strptime(cache, '%Y/%m/%d').weekday() == 5:
							pass
						elif datetime.strptime(cache, '%Y/%m/%d').weekday() == 6:
							pass
						else:
							count += 1
					cache_list = cache.split('/')
					end_date = str(int(cache_list[0]) - 1911) + '/' + cache_list[1] + '/' + cache_list[2]
					
					#理由
					pos_reason = words.find('符合條款')
					reason_string = words[pos_reason:pos_reason + 8].strip()
					reason_string = reason_string.split('：')
					reason = reason_string[1]
					reason = "".join(filter(str.isdigit, reason))
					
					#檢查非代子公司
					pos_main = words.find('主旨')
					main_string = words[pos_main:pos_main + 4]
					main_string = main_string.split('：')
					
					if main_string[1] != '代':
						print(nameofstock[1], nameofstock[2])
						
						#增資減資
						if reason == '11':
							if words[pos_main:pos_main + 100].find('現金增資') != -1:
								reason += 'a'
							elif words[pos_main:pos_main + 100].find('減資') != -1:
								reason += 'b'
							elif words[pos_main:pos_main + 100].find('特別股') != -1:
								reason += 'c'
							else:
								reason = '99'

						#除權息
						if reason == '14':
							cache_string = words[words.find('除權、息類別'):words.find('除權、息類別') + 70]
							cache_string = cache_string.split(':')
							if cache_string[1].find('除權息') != -1:
								reason += 'c'
							elif cache_string[1].find('除權') != -1:
								reason += 'a'
							else:
								reason += 'b'
						
						try:
							result_reason = reason_dict[reason]				
						
						except KeyError:
							if words.find('除權、息類別') != -1:
								cache_string = words[words.find('除權、息類別'):words.find('除權、息類別') + 70]
								cache_string = cache_string.split(':')
								if cache_string[1].find('除權息') != -1:
									reason = '14c'
								elif cache_string[1].find('除權') != -1:
									reason = '14a'
								else:
									reason = '14b'
								result_reason = reason_dict[reason]
							
							else:
								result_reason = '其他2'
						
					else:
						continue
					
					if reason != '9' and reason != '53':
						# series = pd.Series({'1.股票名稱':nameofstock[1], '2.停券起日':start_date, '3.停券迄日':end_date, '4.原因': result_reason, '5.發言日期': nameofstock[2], '6.REASON':reason}, name = nameofstock[0])
						# total_print_list = total_print_list.append(series)
						return_list.append([nameofstock[0], nameofstock[1], start_date, end_date, result_reason, nameofstock[2], reason])
					else:
						continue

				else:
					continue

			else:
				continue

		except Exception as e:
			pass

	return return_list


#測試
# path = 'C:/Users/User/Desktop/pytohncode/news_project/test/20200207'
# result = finding(path)

# result.to_csv('./test.csv', encoding = 'utf_8_sig')
# print('end')


#每日
new_path = '../news_crawler/News'
folder_name = os.listdir(new_path)

if not path.exists('print/data.csv'):
		with open('print/data.csv', 'a', newline='') as csvfile:
			writer = csv.writer(csvfile)
			writer.writerow(['0.股票代號', '1.股票名稱', '2.停券起日','3.停券迄日', '4.原因', '5.發言日期', '6.REASON'])
			csvfile.close()

if not path.exists('print/done_date.csv'):
		with open('print/done_date.csv', 'a', newline='') as csvfile:
			writer = csv.writer(csvfile)
			writer.writerow(['date'])
			csvfile.close()

name_done_list = pd.read_csv('print/done_date.csv')['date']
str_name_done_list = []

for Date in name_done_list:
	str_name_done_list.append(str(Date))

for DateInNew in folder_name:
	if (DateInNew in str_name_done_list):
		pass
	else:
		cache = finding(new_path + '/' + DateInNew)
		print(cache)
		print('-------------')
		for index in cache:
			with open('print/data.csv', 'a', newline='') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow([index[0], index[1], index[2], index[3], index[4], index[5], index[6]])
			csvfile.close()
		with open('print/done_date.csv', 'a', newline='') as csvfile:
			writer = csv.writer(csvfile)
			writer.writerow([DateInNew])
		csvfile.close()

print('end')


#全部
# path = 'C:/Users/User/Desktop/pytohncode/news_project/new'
# folder_name = os.listdir(path)
# total_list = pd.DataFrame()

# for i in folder_name:
# 	cache = finding(path + '/' + i)
# 	total_list = total_list.append(cache)
	
# total_list.to_csv('./index.csv', encoding = 'utf_8_sig')
# print('end')
