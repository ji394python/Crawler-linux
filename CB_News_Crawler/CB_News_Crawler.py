# 爬取公開資訊觀測站的公司債公告網址：https://mops.twse.com.tw/mops/web/t108sb08_1_q2

import os
import re
import sys
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

today_datetime = datetime.today() #跟datetime.now()相同
TWSE_URL = 'https://mops.twse.com.tw'
INDEX_URL = TWSE_URL + '/mops/web/ajax_t108sb08_1'
headers = {
	'Content-Type': 'application/x-www-form-urlencoded',
	'Origin': TWSE_URL,
	'Referer': TWSE_URL + '/mops/web/t108sb08_1_q2',
	'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'
}
data = {
	'encodeURIComponent': '1',
	'run': 'Y',
	'step': '1',
	'firstin': 'true',
	'TYPEK': 'pub' #抓公開發行的股票 (就是全部的意思)
}
# Variables for connection
# Maximum number of reconnection attempts
max_attempt = 10
# Timeout in second
timeout = 6

script_path = os.path.dirname(os.path.abspath('__file__')) # Get parent path
output_path = os.path.join(script_path, 'output')
if not os.path.exists(output_path):
    os.makedirs(output_path)
news_path = os.path.join(output_path, 'bond_news')
if not os.path.exists(news_path):
    os.makedirs(news_path)


# Get source code of the web page
def getWebpage(url):
	r = requests.post(
		url=url,
		allow_redirects=True,
		headers=headers,
		data=data
		)
	if r.status_code != 200:
		print('Invalid URL:', r.url)
		return None
	else:
		return r.text


# Convert year format to A.D.
# Input type: YYY/MM/DD
def transferDate(dateString:str) -> datetime.timetuple:
	'''
		<Ex.> 97/06/13 -> 2008
	'''
	try:
		dateList = [int(num) for num in dateString.split('/')]
		dateList[0] += 1911
		return datetime(*dateList).date()
	except:
		return None


def main():
	start_time = datetime.now()

	# Read input variables
	if len(sys.argv) == 2:
		try:
			data['month'] = sys.argv[1].split('-')[1]
			data['day1'] = sys.argv[1].split('-')[2]
			data['day2'] = sys.argv[1].split('-')[2]
			data['year'] = str(int(sys.argv[1].split('-')[0])-1911)
		except:
			print('Usage:   python convertible_bond_news_daily_scraper.py <year>-<month>-<day>')
			print('Format:  python convertible_bond_news_daily_scraper.py YYYY-MM-DD')
			print('Example: python convertible_bond_news_daily_scraper.py 2021-04-12')
			exit(1) #代表因錯誤而退出
			#exit(0) #代表正常退出
	else:
		print('Usage:   python convertible_bond_news_daily_scraper.py <year>-<month>-<day>')
		print('Format:  python convertible_bond_news_daily_scraper.py YYYY-MM-DD')
		print('Example: python convertible_bond_news_daily_scraper.py 2021-04-12')
		exit(1)

	# Record number of crawled news
	news_count = 0
	# Transfer news Dataframes
	# Create empty transfer news DataFrame (記錄拿的到檔案但沒有資料的)
	empty_transfer_header = ['公司代號', '公司名稱', '申報日期', '申報序號', '主旨']
	empty_transfer_df = pd.DataFrame(columns=empty_transfer_header)
	empty_transfer_count = 0
	# Create invalid transfer news DataFrame for URLs that cannot be connected (記錄拿不到檔案的)
	invalid_transfer_header = ['公司代號', '公司名稱', '申報日期', '申報序號', '主旨']
	invalid_transfer_df = pd.DataFrame(columns=invalid_transfer_header)
	invalid_transfer_count = 0

	# Price news Dataframes
	# Create empty price news DataFrame
	empty_price_header = ['種類', '公司代號', '公司名稱', '申報日期', '申報序號', '主旨']
	empty_price_df = pd.DataFrame(columns=empty_price_header)
	empty_price_count = 0
	# Create invalid price news DataFrame for URLs that cannot be connected
	invalid_price_header = ['種類', '公司代號', '公司名稱', '申報日期', '申報序號', '主旨']
	invalid_price_df = pd.DataFrame(columns=invalid_price_header)
	invalid_price_count = 0

	# Crawl index page
	# Retry when ConnectionError happened (Using status code to judge)
	connect_count = 0
	while connect_count < max_attempt:
		time.sleep(timeout)
		try:
			index_page = getWebpage(INDEX_URL) #挖出所有資料
		except Exception as e:
			print('---------------------------------------------------')
			print('Crawling failed with error:', str(e))
			if connect_count == max_attempt-1:
				print('Failed to reach the index page. Please restart the execution.')
				break
			else:
				print('Reconnect to index page.')
			print('---------------------------------------------------')
			connect_count += 1
			continue
		
		if index_page: #not amount to zero
			print('Start crawling news info on %s...' % sys.argv[1])
			soup = BeautifulSoup(index_page, 'html.parser')
			form1 = soup.find('form', {'name': 'fm'}) #轉(交)換公司債停止轉(交)換公告
			form2 = soup.find('form', {'name': 'formX'}) #轉換公司債轉換價格變更公告

			# Crawl news in form 1
			for table in form1.find_all('table'):
				for tr in table.find_all('tr', {'class': re.compile('odd|even')}):
					tds = tr.find_all('td')
					for i in range(len(tds)):
						# Retrieve posted data when clicking the button
						if i == 5:
							onclick = tds[i].input['onclick']
							onclick_array = onclick.split(';')
							for item in onclick_array:
								if item.split('=')[0] == 'document.fm.dat1.value':
									data['dat1'] = item.split('=')[1].strip('"')
								elif item.split('=')[0] == 'document.fm.bro_date.value':
									data['bro_date'] = item.split('=')[1].strip('"')
								elif item.split('=')[0] == 'document.fm.co_id.value':
									data['co_id'] = item.split('=')[1].strip('"')
								elif item.split('=')[0] == 'document.fm.seq_no.value':
									data['seq_no'] = item.split('=')[1].strip('"')
								elif item.split('=')[0] == 'document.fm.date1.value':
									data['date1'] = item.split('=')[1].strip('"')
								elif item.split('=')[0] == 'document.fm.step.value':
									data['step'] = item.split('=')[1].strip('"')
								elif item.split('=')[0] == 'action':
									news_url = item.split('=')[1].strip('"')
						elif i == 2:
							tds[i] = transferDate(tds[i].text)
						else:
							tds[i] = tds[i].text.strip().rstrip('*')

					# Crawl news content in pop-up
					# Retry when ConnectionError happened
					connect_count = 0
					while connect_count < max_attempt:
						time.sleep(timeout)
						try:
							news_page = getWebpage(TWSE_URL + news_url)
						except Exception as e:
							print('---------------------------------------------------')
							print('Crawling failed with error:', str(e))
							if connect_count == max_attempt-1:
								print('Failed:', tds[1])
								print('Reached maximum number of attempts. Moving to next item.')
								# Save invalid transfer news info
								invalid_transfer_df.loc[invalid_transfer_count] = [*tds[:-1]]
								invalid_transfer_count += 1
								break
							else:
								print('Reconnect:', tds[1])
							print('---------------------------------------------------')
							connect_count += 1
							continue

						if news_page:
							print('Crawling news:', tds[1])
							try:
								news_soup = BeautifulSoup(news_page, 'html.parser')
								trs = news_soup.find_all('table', 'noBorder')[1].find_all('tr')[:-1]
								text = ''
								for i in range(len(trs)):
									text += trs[i].text + '\n'

								# Write transfer news to text file
								text_path = os.path.join(news_path, tds[2].strftime('%Y%m%d'))
								if not os.path.exists(text_path):
									os.makedirs(text_path)
								with open(os.path.join(text_path, tds[0] + '_' + tds[1] + '_' + tds[2].strftime('%Y%m%d') + '_' + tds[3] + '.txt'), 'w', encoding='utf_8_sig') as f:
									f.write(text)
								news_count += 1
							except Exception as e:
								print('---------------------------------------------------')
								print('Crawling failed with error:', str(e))
								print('Failed:', tds[1])
								print('The news page is connected without required information.')
								print('---------------------------------------------------')
								# Save empty transfer news info
								empty_transfer_df.loc[empty_transfer_count] = [*tds[:-1]]
								empty_transfer_count += 1

						else:
							print('---------------------------------------------------')
							print('Failed:', tds[1])
							print('The news page is connected without required information.')
							print('---------------------------------------------------')
							# Save empty transfer news info
							empty_transfer_df.loc[empty_transfer_count] = [*tds[:-1]]
							empty_transfer_count += 1
						break

			# Crawl news in form 2
			for table in form2.find_all('table'):
				for tr in table.find_all('tr', {'class': re.compile('odd|even')}):
					tds = tr.find_all('td')
					# Set type of news as none if not exist
					if len(tds) == 6:
						tds.insert(0, None)

					for i in range(len(tds)):
						if tds[i]:
							# Retrieve posted data when clicking the button
							if i == 6:
								onclick = tds[i].input['onclick']
								onclick_array = onclick.split(';')[0].split(',')
								data['co_id'] = onclick_array[2].strip('"')
								data['seq_no'] = onclick_array[3].strip('"')
								data['date1'] = onclick_array[4].split('"')[1]
								data['step'] = '2'

								# Calculate value attached in the news URL
								x = int(onclick_array[1])
								actionValue = 0
								if x >= 1 and x <= 3:
									actionValue = (x - 1) * 5 + (int(onclick_array[5].split('"')[1]) - 2) + 5
								else:
									actionValue = (x - 4) + 20
								if actionValue < 10:
									news_url = '/mops/web/ajax_t120sb0' + str(actionValue)
								else:
									news_url = '/mops/web/ajax_t120sb' + str(actionValue)
								if x >= 7 and x <= 9:
									data['pub_class'] = str(onclick_array[5].split('"')[1])

							elif i == 3:
								tds[i] = transferDate(tds[i].text)
							else:
								tds[i] = tds[i].text.strip().rstrip('*')

					# Crawl news content in pop-up
					# Retry when ConnectionError happened
					connect_count = 0
					while connect_count < max_attempt:
						time.sleep(timeout)
						try:
							news_page = getWebpage(TWSE_URL + news_url)
						except Exception as e:
							print('---------------------------------------------------')
							print('Crawling failed with error:', str(e))
							if connect_count == max_attempt-1:
								print('Failed:', tds[2])
								print('Reached maximum number of attempts. Moving to next item.')
								# Save invalid price news info
								invalid_price_df.loc[invalid_price_count] = [*tds[:-1]]
								invalid_price_count += 1
								break
							else:
								print('Reconnect:', tds[2])
							print('---------------------------------------------------')
							connect_count += 1
							continue

						if news_page:
							print('Crawling news:', tds[2])
							try:
								news_soup = BeautifulSoup(news_page, 'html.parser')
								trs = news_soup.find_all('table', 'noBorder')[1].find_all('tr')
								text = ''
								for i in range(len(trs)):
									text += trs[i].text + '\n'

								# Write price news to text file
								text_path = os.path.join(news_path, tds[3].strftime('%Y%m%d'))
								if not os.path.exists(text_path):
									os.makedirs(text_path)
								with open(os.path.join(text_path, tds[1] + '_' + tds[2] + '_' + tds[3].strftime('%Y%m%d') + '_' + tds[4] + '.txt'), 'w', encoding='utf_8_sig') as f:
									f.write(text)
								news_count += 1
							except Exception as e:
								print('---------------------------------------------------')
								print('Crawling failed with error:', str(e))
								print('Failed:', tds[2])
								print('The news page is connected without required information.')
								print('---------------------------------------------------')
								# Save empty price news info
								empty_price_df.loc[empty_price_count] = [*tds[:-1]]
								empty_price_count += 1

						else:
							print('---------------------------------------------------')
							print('Failed:', tds[2])
							print('The news page is connected without required information.')
							print('---------------------------------------------------')
							# Save empty price news info
							empty_price_df.loc[empty_price_count] = [*tds[:-1]]
							empty_price_count += 1
						break
		# Returned index page HTML is empty or does not contain required info
		else:
			print('---------------------------------------------------')
			print('The index page is connected without required information.')
			print('---------------------------------------------------')
			break

		# Save transfer news info
		# Write empty transfer news info to CSV sorted by release date if any
		if not empty_transfer_df.empty:
			empty_transfer_df.sort_values(by=['申報日期'], ascending=False, inplace=True)
			empty_transfer_path = os.path.join(news_path, 'transfer_empty_' + today_datetime.strftime('%Y-%m-%d') + '.csv')
			if not os.path.isfile(empty_transfer_path):
				empty_transfer_df.to_csv(empty_transfer_path, encoding='utf_8_sig', index=False)
			else:
				empty_transfer_df.to_csv(empty_transfer_path, mode='a', header=False, encoding='utf_8_sig', index=False)
		# Write invalid transfer news info to CSV sorted by release date if any
		if not invalid_transfer_df.empty:
			invalid_transfer_df.sort_values(by=['申報日期'], ascending=False, inplace=True)
			invalid_transfer_path = os.path.join(news_path, 'transfer_invalid_' + today_datetime.strftime('%Y-%m-%d') + '.csv')
			if not os.path.isfile(invalid_transfer_path):
				invalid_transfer_df.to_csv(invalid_transfer_path, encoding='utf_8_sig', index=False)
			else:
				invalid_transfer_df.to_csv(invalid_transfer_path, mode='a', header=False, encoding='utf_8_sig', index=False)

		# Save price news info
		# Write empty price news info to CSV sorted by release date if any
		if not empty_price_df.empty:
			empty_price_df.sort_values(by=['申報日期'], ascending=False, inplace=True)
			empty_price_path = os.path.join(news_path, 'price_empty_' + today_datetime.strftime('%Y-%m-%d') + '.csv')
			if not os.path.isfile(empty_price_path):
				empty_price_df.to_csv(empty_price_path, encoding='utf_8_sig', index=False)
			else:
				empty_price_df.to_csv(empty_price_path, mode='a', header=False, encoding='utf_8_sig', index=False)
		# Write invalid price news info to CSV sorted by release date if any
		if not invalid_price_df.empty:
			invalid_price_df.sort_values(by=['申報日期'], ascending=False, inplace=True)
			invalid_price_path = os.path.join(news_path, 'price_invalid_' + today_datetime.strftime('%Y-%m-%d') + '.csv')
			if not os.path.isfile(invalid_price_path):
				invalid_price_df.to_csv(invalid_price_path, encoding='utf_8_sig', index=False)
			else:
				invalid_price_df.to_csv(invalid_price_path, mode='a', header=False, encoding='utf_8_sig', index=False)

		print('===================================================')
		print('Crawling finished.')
		print('Number of news retrieved: %d' % (news_count))
		print('Number of empty transfer news: %d' % (len(empty_transfer_df.index)))
		print('Number of invalid transfer news: %d' % (len(invalid_transfer_df.index)))
		print('Number of empty price news: %d' % (len(empty_price_df.index)))
		print('Number of invalid price news: %d' % (len(invalid_price_df.index)))
		break
	end_time = datetime.now()
	time_diff = end_time - start_time
	print('Execution time:', str(time_diff))
	print('===================================================')


if __name__ == '__main__':
	main()
