# -*- coding: utf-8 -*-
# 爬取櫃買中心的公司債基本資料網址：https://www.tpex.org.tw/web/bond/publish/convertible_bond_search/memo.php?l=zh-tw
# 爬取原始新聞

"""
@author: Denver Liu

Last Modified：08/26

"""
import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import traceback
import log_manager as log
import json

# Get source code of the web page
def getWebpage(url, download=False):
	r = requests.get(
		url=url,
		allow_redirects=True,
		headers=headers
	)
	if r.status_code != 200:
		print('Invalid URL:', r.url)
		return None
	else:
		if download:
			return r.content
		else:
			return r.text


#民國轉西元
# Convert year format to A.D. (轉換民國 -> 西元)
# Input type: YYY/MM/DD
def transferDate(dateString: str) -> datetime.timetuple:
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
	try:
		start_time = datetime.now()
		# Download bond list CSV
		# Retry when ConnectionError happened
		connect_count = 0
		while connect_count < max_attempt:
			time.sleep(timeout)
			try:
				content = getWebpage(BOND_LIST_URL, download=True)
			except Exception as e:
				print('---------------------------------------------------')
				print('Crawling failed with error:', str(e))
				log.processLog('---------------------------------------------------')
				log.processLog(f'Crawling failed with error:{str(e)}')
				if connect_count == max_attempt-1:
					print('Failed to download bond list CSV.')
					log.processLog('Failed to download bond list CSV.')
					break
				else:
					print('Reconnect to bond list URL.')
					log.processLog('Reconnect to bond list URL.')
				print('---------------------------------------------------')
				log.processLog('---------------------------------------------------')
				connect_count += 1
				continue

			# Save bond list CSV
			open(template, 'wb').write(content)
			# Read bond list as DataFrame
			bond_list_df = pd.read_csv(template, skiprows=3, encoding='cp950')
			print('Start crawling bond info...')
			log.processLog('Start crawling bond info...')

			# Create bond DataFrame
			bond_header = ['債券代碼', '債券簡稱', '債券中文名稱', '債券英文名稱', '債券期別', '募集方式',
						'申請日期', '核准日期', '發行日期', '發行期限', '到期日期', '下櫃日期',
						'股東會或董事會日期', '債券掛牌情形', '掛牌/發行地點', '掛牌日期', '發行幣別', '發行日匯率',
						'發行人', '本次價格重設日期', '下次價格重設日期', 'CFI代號', '國際編碼', '申請發行總額',
						'實際發行總額', '本月轉換張數', '本月轉為換股權利證書股數', '本月轉為普通股股數', '本月償還本金金額',
						'本月買回張數', '本月賣回張數', '本月底發行餘額', '按時還本', '處理情形', '違約金額',
						'處理結果', '發行面額', '發行張數', '發行價格', '票面利率', '債息基準日',
						'發行時轉換價格', '轉換溢價率', '轉換起日', '轉換迄日', '轉換匯率',
						'最新轉換價格', '最近轉換價格生效日期', '債息對照表', '擔保情形', '還本敘述', '賣回條件',
						'賣回收益率', '買回條件', '買回收益率', '下次賣回日期', '下次賣回價格', '承銷機構',
						'受託人', '簽證機構', '過戶機構', '信用評等等級', '本月發行餘額變動日期', '本月發行餘額變動原因',
						'限制條款', '備註', '認購對象', '發行辦法', '債券網址']
			bond_df = pd.DataFrame(columns=bond_header)
			bond_count = 0
			# Create empty bond DataFrame
			# 網址連的到，但表格資訊為空的債券放這裡 -> 目前爬到空的或任何一列不對都會直接進來 有點太強制了
			empty_header = ['債券代碼', '債券簡稱', '發行人', '發行日期',
							'到期日期', '發行期限', '發行總面額', '債券網址']
			empty_df = pd.DataFrame(columns=empty_header)
			empty_count = 0
			# Create invalid bond DataFrame for URLs that cannot be connected
			# 網址連不到的債券會放到這裡
			invalid_header = ['債券代碼', '債券簡稱', '發行人', '發行日期',
							'到期日期', '發行期限', '發行總面額', '債券網址']
			invalid_df = pd.DataFrame(columns=invalid_header)
			invalid_count = 0

			# Crawl bond info from each bond URL
			for bondName, bondURL in zip(bond_list_df['Short Name'], bond_list_df['Bond Database']):
				# Retry when ConnectionError happened
				connect_count = 0
				while connect_count < max_attempt: #每個債券最多重爬十次
					time.sleep(timeout)
					try:
						bond_page = getWebpage(bondURL)
					except Exception as e:
						print('---------------------------------------------------')
						print('Crawling failed with error:', str(e))
						log.processLog('---------------------------------------------------')
						log.processLog(f'Crawling failed with error:{str(e)}')
						if connect_count == max_attempt-1:
							print('Failed:', bondName)
							print('Reached maximum number of attempts. Moving to next item.')
							log.processLog(f'Failed:{bondName}')
							log.processLog('Reached maximum number of attempts. Moving to next item.')
							# Save invalid bond info
							invalid_df.loc[invalid_count] = bond_list_df.loc[bond_count+empty_count+invalid_count]
							invalid_count += 1
							break
						else:
							print('Reconnect:', bondName)
							log.processLog(f'Reconnect:{bondName}')
						print('---------------------------------------------------')
						log.processLog('---------------------------------------------------')
						connect_count += 1
						continue

					if bond_page:
						print('Crawling:', bondName)
						log.processLog(f'Crawling:{bondName}')
						try:
							soup = BeautifulSoup(bond_page, 'html.parser')
							rows = soup.find('table', 'hasBorder').find_all('tr')
							for index, row in enumerate(rows):
								# index是指逐行的結果 (因為有固定格式) -> 因此要特別留意格式不相同的
								if index == 0:
									bond_yrn = [int(s) for s in row.find_all('td')[0].text.split() if s.isdigit()][0] #取得第幾期債券
									offering_method = row.find_all('td')[1].text.split('：')[1].strip() #取得該公司債募集方式
								elif index == 1:
									application_date = transferDate(row.find_all('td')[0].text.split('：')[1]) #申請日期
									approved_date = transferDate(row.find_all('td')[1].text.split('：')[1]) #核准日期
								elif index == 2:
									issued_date = transferDate(row.find_all('td')[0].text.split('：')[1]) #發行日期
									# Use year as unit of bond period
									bond_period = [float(s) for s in re.split(' |：|年', row.find_all('td')[1].text) if s.isdigit()] #發行期限
									# re.findall('[0-9]+',row.find_all('td')[1].text) 或許可以用這個取代
									bond_period = bond_period[0] + round(bond_period[1]/12, 1)
								elif index == 3:
									delivery_date = transferDate(row.find_all('td')[0].text.split('：')[1]) #到期日期
									delisted_date = transferDate(row.find_all('td')[1].text.split('：')[1]) #下櫃日期
								elif index == 4:
									shareholder_date = transferDate(row.find_all('td')[0].text.split('：')[1]) #公司決議私募有價證券股東會或董事會日期
								elif index == 5:
									listed_type = row.find_all('td')[0].text.split('：')[1].strip() #債券掛牌情形
									listed_location = row.find_all('td')[1].text.split('：')[1].strip() #掛牌/發行地點
								elif index == 6:
									listed_date = transferDate(row.find_all('td')[0].text.split('：')[1]) #掛牌日期
									issued_currency = row.find_all('td')[1].text.split()[0].split('：')[1] #發行幣別
									issued_exchange_rate = float(row.find_all('td')[1].text.split()[-1]) #發行日匯率 -> 台幣的話會是0
								elif index == 7:
									bond_id = row.find_all('td')[0].text.split('：')[1] #債券代碼
									bond_abbrev = row.find_all('td')[1].text.split('：')[1].strip() #債券簡稱
								elif index == 8:
									bond_chi_name = row.find_all('td')[0].text.split('：')[1].strip() #債券中文名稱
									issuer = row.find_all('td')[1].text.split('：')[1].strip() #發行人
								elif index == 9:
									bond_eng_name = row.find_all('td')[0].text.split('：')[1].strip() #債券英文名稱
								elif index == 10:
									price_reset_date = transferDate(row.find_all('td')[0].text.split('：')[1].split('民國')[1].strip()) #本次價格重設日期
									next_price_reset_date = transferDate(row.find_all('td')[1].text.split('：')[1].split('民國')[1].strip()) #下次價格重設日期
								elif index == 11:
									CFI = row.find_all('td')[0].text.split('：')[1] #CFI代號
									ISIN = row.find_all('td')[1].text.split('：')[1] #國際編碼
								elif index == 12:
									applied_issued_amount = int(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', '')) #申請發行總額
								elif index == 13:
									actual_issued_amount = int(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', '')) #實際發行總額
								elif index == 14:
									transfered_bond = int(row.find_all('td')[0].text.split('：')[1].split('張')[0].replace(',', '')) #本月受理轉(交)換之公司債張數
								elif index == 15:
									transfered_entitled_cert = int(row.find_all('td')[0].text.split('：')[1].split('股')[0].replace(',', '')) #本月轉(交)換為債券換股權利證書股數
								elif index == 16:
									transfered_stock = int(row.find_all('td')[0].text.split('：')[1].split('股')[0].replace(',', '')) #本月轉(交)換為普通股股數(未變更登記)
								elif index == 17:
									repaid_amount = int(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', '')) #本月償還本金金額
								elif index == 18:
									buyback_bond = int(row.find_all('td')[0].text.split('：')[1].split('張')[0].replace(',', '')) #本月公司買回(註銷)張數
								elif index == 19:
									sold_bond = int(row.find_all('td')[0].text.split('：')[1].split('張')[0].replace(',', '')) #本月行使賣回權張數
								elif index == 20:
									remaining_issued_amount = int(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', '')) #本月底發行餘額
								elif index == 21:
									repay_ontime = row.find_all('td')[0].text.split('，')[0].split('：')[1] #是否依約定按時還本
									repay_process_detail = row.find_all('td')[0].text.split('，')[1].split('：')[1].strip() #處理情形
								elif index == 22:
									breach_penalty = int(row.find_all('td')[0].text.split()[1].replace(',', '')) #違約金額
									penalty_result = row.find_all('td')[0].text.split()[-1].split('：')[-1].strip() #處理結果
								elif index == 23:
									par_value = int(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', '')) #發行面額
									issued_number = int(row.find_all('td')[1].text.split('：')[1].split('張')[0].replace(',', '')) #發行張數
								elif index == 24:
									issued_price = float(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', '')) #發行價格
								elif index == 25:
									coupon_rate = float(row.find_all('td')[0].text.split('：')[1].split('%')[0]) #票面利率
									exdividend_date = row.find_all('td')[1].text.split('：')[1] #債息基準日
								elif index == 26:
									issued_conversion_price = float(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', '')) #發行時轉(交)換價格
									convertible_premium_rate = float(row.find_all('td')[1].text.split('：')[1].split('%')[0]) #轉換溢價率
								elif index == 27:
									conversion_start_date = transferDate(row.find_all('td')[0].text.split('：')[1].split('～')[0]) #轉交換期間 (起日)
									conversion_end_date = transferDate(row.find_all('td')[0].text.split('：')[1].split('～')[1])   #轉交換期間 (迄日) 
									conversion_exchange_rate = float(row.find_all('td')[1].text.split('：')[1]) #轉換匯率
								elif index == 28:
									latest_conversion_price = float(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', '')) #最新轉(交)換價格
									latest_convert_date = transferDate(row.find_all('td')[1].text.split('：')[1]) #最近轉(交)換價格生效日期
								elif index == 29:
									dividend_table = row.find_all('td')[0].text.split('：')[1] #債息對照表內容
									# Retrieve download link if table name is shown
									if dividend_table:
										dividend_table = TWSE_URL + row.find_all('td')[0].find('a', href=True)['href'] #貼網址?
								elif index == 30:
									secured_detail = row.find_all('td')[0].text.split('：')[1].strip() #擔保銀行
								elif index == 31:
									repay_detail = row.find_all('td')[0].text.split('：')[1].strip() #還本敘述
								elif index == 32:
									sell_condition = row.find_all('td')[0].text.split('：')[1].strip() #債券賣回權條件
								elif index == 33:
									sell_earnings_rate = float(row.find_all('td')[0].text.split('：')[1].split('%')[0]) #賣回權收益率
								elif index == 34:
									buyback_condition = row.find_all('td')[0].text.split('：')[1].strip() #債券買回權條件
								elif index == 35:
									buyback_earnings_rate = float(row.find_all('td')[0].text.split('：')[1].split('%')[0]) #買回權收益率
								elif index == 36:
									next_sell_date = transferDate(row.find_all('td')[0].text.split('：')[1]) #下一次賣回權日期
									next_sell_price = float(row.find_all('td')[1].text.split('：')[1].split('%')[0]) #下一次賣回權價格
								elif index == 37:
									underwriting_institute = row.find_all('td')[0].text.split('：')[1].strip() #承銷機構
								elif index == 38:
									consignee = row.find_all('td')[0].text.split('：')[1].strip() #受託人
									attest_institute = row.find_all('td')[1].text.split('：')[1].strip() #簽證機構
								elif index == 39:
									transfer_institute = row.find_all('td')[0].text.split('：')[1].strip() #過戶機構
									credit_rating = row.find_all('td')[1].text.split('：')[1].strip() #信用評等等級
								elif index == 40:
									issued_change_date = transferDate(row.find_all('td')[0].text.split('：')[1]) #本月發行餘額變動日期
								elif index == 41:
									issued_change_reason = row.find_all('td')[0].text.split('：')[1].strip() #本月發行餘額變動原因
								elif index == 42:
									restriction = row.find_all('td')[0].text.split('：')[1].strip() #限制條款內容
								elif index == 43:
									remark = row.find_all('td')[0].text.split('：')[1].strip() #備註
								elif index == 44:
									caller = row.find_all('td')[0].text.split('：')[1].strip() #認購對象
								elif index == len(rows)-1:
									regulation = TWSE_URL + row.find_all('td')[0].find('a', href=True)['href'] #發行辦法之內容

							# Save bond info to DataFrame
							bond_df.loc[bond_count] = [bond_id, bond_abbrev, bond_chi_name, bond_eng_name, bond_yrn, offering_method,
													application_date, approved_date, issued_date, bond_period, delivery_date, delisted_date,
													shareholder_date, listed_type, listed_location, listed_date, issued_currency, issued_exchange_rate,
													issuer, price_reset_date, next_price_reset_date, CFI, ISIN, applied_issued_amount,
													actual_issued_amount, transfered_bond, transfered_entitled_cert, transfered_stock, repaid_amount,
													buyback_bond, sold_bond, remaining_issued_amount, repay_ontime, repay_process_detail, breach_penalty,
													penalty_result, par_value, issued_number, issued_price, coupon_rate, exdividend_date,
													issued_conversion_price, convertible_premium_rate, conversion_start_date, conversion_end_date, conversion_exchange_rate,
													latest_conversion_price, latest_convert_date, dividend_table, secured_detail, repay_detail, sell_condition,
													sell_earnings_rate, buyback_condition, buyback_earnings_rate, next_sell_date, next_sell_price, underwriting_institute,
													consignee, attest_institute, transfer_institute, credit_rating, issued_change_date, issued_change_reason,
													restriction, remark, caller, regulation, bondURL]
							bond_count += 1
						except Exception as e:
							print('---------------------------------------------------')
							print('Crawling failed with error:', str(e))
							print('Failed:', bondName)
							print('The bond page is connected without required information.')
							print('---------------------------------------------------')
							log.processLog('---------------------------------------------------')
							log.processLog(f'Crawling failed with error:{str(e)}')
							log.processLog(f'Failed:{bondName}')
							log.processLog('The bond page is connected without required information.')
							log.processLog('---------------------------------------------------')
							# Save bond info of empty URLs
							empty_df.loc[empty_count] = bond_list_df.loc[bond_count+empty_count+invalid_count]
							empty_count += 1
					else:
						print('---------------------------------------------------')
						print('Failed:', bondName)
						print('The bond page is connected without required information.')
						print('---------------------------------------------------')
						log.processLog('---------------------------------------------------')
						log.processLog(f'Failed:{bondName}')
						log.processLog('The bond page is connected without required information.')
						log.processLog('---------------------------------------------------')
						# Save bond info of empty URLs
						empty_df.loc[empty_count] = bond_list_df.loc[bond_count+empty_count+invalid_count]
						empty_count += 1
					break

			# Write bond info to CSV sorted by bond ID
			bond_df.sort_values(by=['債券代碼'], ascending=True, inplace=True)
			bond_df.to_csv(os.path.join(output_dir_path, "CB_OTC_" + today_datetime + '.csv'), encoding='utf_8_sig', index=False)

			# Write empty bond info to CSV sorted by bond ID if any
			if not empty_df.empty:
				empty_df.sort_values(by=['債券代碼'], ascending=True, inplace=True)
				empty_df.to_csv(os.path.join(output_dir_path, "CB_OTC_" + today_datetime + '_empty.csv'), encoding='utf_8_sig', index=False)

			# Write invalid bond info to CSV sorted by bond ID if any
			if not invalid_df.empty:
				invalid_df.sort_values(by=['債券代碼'], ascending=True, inplace=True)
				invalid_df.to_csv(os.path.join(output_dir_path, "CB_OTC_" + today_datetime + '_invalid.csv'), encoding='utf_8_sig', index=False)

			print('===================================================')
			print('Crawling finished.')
			print('Number of bonds retrieved: %d' % (len(bond_df.index)))
			print('Number of empty bonds: %d' % (len(empty_df.index)))
			print('Number of invalid bonds: %d' % (len(invalid_df.index)))
			log.processLog('===================================================')
			log.processLog('Crawling finished.')
			log.processLog('Number of bonds retrieved: %d' % (len(bond_df.index)))
			log.processLog('Number of empty bonds: %d' % (len(empty_df.index)))
			log.processLog('Number of invalid bonds: %d' % (len(invalid_df.index)))
			break
		end_time = datetime.now()
		time_diff = end_time - start_time
		print('Execution time:', str(time_diff))
		print('===================================================')
		log.processLog(f'Execution time:{str(time_diff)}')
		log.processLog('===================================================')

	except Exception as e:
		log.processLog('發生錯誤:請查看error.log')
		traceback.print_exc()
		log.errorLog(traceback.format_exc())



if __name__ == '__main__':
	try:

		TWSE_URL = 'https://mops.twse.com.tw'
		BOND_LIST_URL = 'https://www.tpex.org.tw/web/bond/publish/convertible_bond_search/memo_download.php?d=issue.txt'
		headers = {
			'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36'
		}
		# Variables for connection
		# Maximum number of reconnection attempts
		max_attempt = 10
		# Timeout in second
		timeout = 3

		today_datetime = datetime.today().strftime('%Y-%m-%d')


		#路徑控制
		rootPath = json.load(open('set.json','r+'))['output_dir_path']
		output_dir_path = f'{rootPath}/CB_Specs'
		if not os.path.exists(output_dir_path):
			os.makedirs(output_dir_path)
		template = os.path.join(output_dir_path, 'CB_OTC_TEMP_' + today_datetime + '.csv')
		main()
		os.system(f'rm -f {template}')

	except Exception as e:
		log.processLog('發生錯誤:請查看error.log')
		traceback.print_exc()
		log.errorLog(traceback.format_exc())
