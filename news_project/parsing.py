# -*- coding: utf-8 -*-
"""
Author: Denver Liu

Last Updated: 2021/07/08

"""
import log_manager as log
import os
import csv
import time
import os.path
from os import path
from datetime import datetime
import pandas as pd
from pandas_datareader import data as pdr 
import traceback
import re

def finding(folderPath:str) -> list: 
	'''
		讀取 ../../ShareDiskE/News_Stocks/${Date}/${File} 進行剖析
		並採取漏斗式的篩選原則 
	'''
	
	#呼叫全域變數 - 其實不需要也沒關係,為了pythonic
	global reasonDict
	global stockNameList
	global holidayList
	global workdayList

	allFileList = os.listdir(folderPath) 
	return_list = []

	for i in allFileList: # ../../ShareDiskE/News_Stocks/${Date}/${File}
		#nameoffile = i
		path = folderPath + '/' + i
		file = open(path, 'r',encoding='utf-8-sig')
		log.processLog(f'=== 剖析{path}')
		#print(path)
		words = str(file.read())
		#nameofstock[0] = 代號, nameofstock[1] = 中文名, nameofstock[2] = 發言時間
		nameofstock = i.split('_')
		#股票代馬內
			#會停止過戶
				#排除代子公司、代重要子公司、第一個字為代
					#排除
		try:
			if nameofstock[1] in stockNameList: #在股票代碼內
				#print(nameofstock)
				if words.find('停止過戶起始日期') != -1: #有這幾個字
					#日期
					pos_trade_date = words.find('停止過戶起始日期')
				#	print(pos_trade_date)
					date_string = words[pos_trade_date:words.find('\n',pos_trade_date)].strip() ##等等來Split 幹
					if date_string.find('~') != -1:
						date_string = date_string[:date_string.find('~')]
					date_string = date_string.split(':')
					cache_date_string = date_string[1].split('/')
					print(cache_date_string,path)
					if len(cache_date_string) == 1:
						cache_date_string1 = date_string[1].split('年')
						cache_date_string2 = cache_date_string1[1].split('月')
						cache_date_string3 = cache_date_string2[1].split('日')
						cache_date_string = ["".join(filter(str.isdigit, cache_date_string1[0])), cache_date_string2[0], cache_date_string3[0]]
					year = str(int(re.sub('[^0-9]','',cache_date_string[0])) + 1911)
					cache_date_list = year + '/' + re.sub('[^0-9]','',cache_date_string[1]) + '/' + re.sub('[^0-9]','',cache_date_string[2])
					#cache_date_list = re.sub('[^0-9]','',cache_date_string[2])
					time_stamp = int(time.mktime(time.strptime(cache_date_list, '%Y/%m/%d')))
					count = 0
				
				#起迄日為法規規定：https://www.twse.com.tw/ch/products/publication/download/0001001837.pdf
					#停券起日
					while count < 6:
						time_stamp -= 86400 #轉成前一天
						cache = time.strftime('%Y/%m/%d', time.localtime(time_stamp))
						if str(cache) in holidayList:
							pass
						elif str(cache) in workdayList:
							count += 1
						elif datetime.strptime(cache, '%Y/%m/%d').weekday() in [5,6]:
							pass
						else:
							count += 1
					cache_list = cache.split('/')
					start_date = str(cache_list[0]) + '/' + cache_list[1] + '/' + cache_list[2]
					#停券迄日
					count = 0
					while count < 3:
						time_stamp += 86400
						cache = time.strftime('%Y/%m/%d', time.localtime(time_stamp))
						if str(cache) in holidayList:
							pass
						elif str(cache) in workdayList:
							count += 1
						elif datetime.strptime(cache, '%Y/%m/%d').weekday() in [5,6]:
							pass
						else:
							count += 1
					cache_list = cache.split('/')
					end_date = str(cache_list[0]) + '/' + cache_list[1] + '/' + cache_list[2]
					
					#理由
					#理由的條款可參考此http://www.selaw.com.tw/LawContent.aspx?LawID=G0100104&Hit=1
					pos_reason = words.find('符合條款')
					reason_string = words[pos_reason:words.find('\n',pos_reason)].strip()
					reason_string = reason_string.split('：')
					reason = reason_string[1]
					reason = "".join(filter(str.isdigit, reason))
					
					#檢查非代子公司
					pos_main = words.find('主旨')
					main_string = words[pos_main:pos_main + 4]
					main_string = main_string.split('：')
					#儲存主旨
					mainContent = words[pos_main+3:words.find('\n',pos_main+1)]

					for checkWord in ['代子公司','公告子公司','重要子公司','本公司代','富邦金控代','台新金控代']:
						if checkWord in mainContent:
							check = False
							break
						else:
							check = True

					if (check & (main_string[1] != '代')):
						
						if ( (mainContent.count('子公司') == 1) & (mainContent.count('電子公司') == 0) ):
							continue
						else:

							#此if不能排除 ....子公司-xxx電子公司招開股東大會...主旨
						#	print(nameofstock[1], nameofstock[2])
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
								result_reason = reasonDict[reason]				
							
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
									result_reason = reasonDict[reason]
								
								else:
									result_reason = '其他2'
					else:
						continue
					
					if reason != '9' and reason != '53':
						return_list.append([nameofstock[0], nameofstock[1], start_date, end_date, result_reason, re.sub('-','/',nameofstock[2]), reason,mainContent])
					else:
						continue

		except IndexError:
			pass
		except Exception as e:
			log.errorLog(traceback.format_exc())
			traceback.print_exc()
			pass

	return return_list


if __name__ == '__main__':

	log.processLog('【開始執行News_Stocks爬蟲專案】 parsing.py')
	start = time.perf_counter_ns()
	try:
		if os.path.exists('../../ShareDiskE/News_Stocks/print') == False:
			os.mkdir('../../ShareDiskE/News_Stocks/print')
			log.processLog('建立資料夾： "../../ShareDiskE/News_Stocks/print" ')

		#理由的條款可參考此http://www.selaw.com.tw/LawContent.aspx?LawID=G0100104&Hit=1
		reasonDict = {
			'11a': '增資', '11b': '減資', '11c': '其他利益',
			'14a': '除權', '14b': '除息','14c': '除權息',
			'17': '股東常會', '32': '股東常會',
			'51': '金控/股份轉換', '99': '其他1'
			}

		log.processLog('讀取先驗資料檔: 股票代號、假日資訊')
		#讀取股票代號:https://www.twse.com.tw/zh/page/products/stock-code2.html
		stockNameList = pd.read_csv('predata/stock_index_confirm.csv')['name'].values.tolist() 

		#讀取台灣例假日資訊:https://www.twse.com.tw/zh/holidaySchedule/holidaySchedule
		holidayList = pd.read_csv('predata/holiday.csv')['date'].apply(lambda x: time.strftime('%Y/%m/%d',time.strptime(x,'%Y/%m/%d'))) 
		workdayList = pd.read_csv('predata/workday.csv')['date'].apply(lambda x: time.strftime('%Y/%m/%d',time.strptime(x,'%Y/%m/%d'))) 

		#每日
		new_path = '../../ShareDiskE/News_Stocks/'
		folderName = os.listdir(new_path)

		if not path.exists('../../ShareDiskE/News_Stocks/print/data.csv'):
			log.processLog('建立整合資料檔: data.csv')
			with open('../../ShareDiskE/News_Stocks/print/data.csv', 'a', newline='',encoding='utf-8-sig') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow(['股票代號', '股票名稱', '停券起日','停券迄日', '原因', '發言日期', 'REASON','主旨'])
				csvfile.close()

		if not path.exists('../../ShareDiskE/News_Stocks/print/done_date.csv'):
			log.processLog('建立日期紀錄檔: done_date.csv')
			with open('../../ShareDiskE/News_Stocks/print/done_date.csv', 'a', newline='',encoding='utf-8-sig') as csvfile:
				writer = csv.writer(csvfile)
				writer.writerow(['date'])
				csvfile.close()

		#讀取已剖析過的日期紀錄檔
		log.processLog('讀取日期資料檔: done_date.csv')
		completeDate = pd.read_csv('../../ShareDiskE/News_Stocks/print/done_date.csv')['date'].values.tolist()
		completeDate.append('print')
		completeDate.append('log')

		for DateInNew in folderName:
			if (DateInNew not in completeDate):
				log.processLog(f'開始剖析 {DateInNew} 之公告')
				cache = finding(new_path + '/' + DateInNew)
				#print(cache)
				print('-------------')
				for index in cache:
					with open('../../ShareDiskE/News_Stocks/print/data.csv', 'a', newline='',encoding='utf-8-sig') as csvfile:
						writer = csv.writer(csvfile)
						writer.writerow([index[0], index[1], index[2], index[3], index[4], index[5], index[6],index[7]])
					csvfile.close()
				with open('../../ShareDiskE/News_Stocks/print/done_date.csv', 'a', newline='',encoding='utf-8-sig') as csvfile:
					writer = csv.writer(csvfile)
					writer.writerow([DateInNew])
				csvfile.close()

		end = time.perf_counter_ns() 
		log.processLog(f'【結束程序】 parsing.py - 執行時間:{(end-start)/10**9}')
		log.processLog('==============================================================================================')

		print('end')

	except Exception as e:
		log.processLog('發生錯誤:請查看error.log')
		traceback.print_exc()
		log.errorLog(traceback.format_exc())
