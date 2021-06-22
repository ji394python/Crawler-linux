import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

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
script_path = os.path.dirname(os.path.abspath(__file__))
template_path = os.path.join(script_path, 'template')
if not os.path.exists(template_path):
	os.makedirs(template_path)
template = os.path.join(template_path, 'convertible_bond_publish_' + today_datetime + '.csv')
output_path = os.path.join(script_path, 'output')
if not os.path.exists(output_path):
	os.makedirs(output_path)
info_path = os.path.join(output_path, 'bond_info')
if not os.path.exists(info_path):
	os.makedirs(info_path)


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


# Convert year format to A.D.
# Input type: YYY/MM/DD
def transferDate(dateString):
	try:
		dateList = [int(num) for num in dateString.split('/')]
		dateList[0] += 1911
		return datetime(*dateList).date()
	except:
		return None


def main():
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
			if connect_count == max_attempt-1:
				print('Failed to download bond list CSV.')
				break
			else:
				print('Reconnect to bond list URL.')
			print('---------------------------------------------------')
			connect_count += 1
			continue

		# Save bond list CSV
		open(template, 'wb').write(content)
		# Read bond list as DataFrame
		bond_list_df = pd.read_csv(template, skiprows=3, encoding='cp950')
		print('Start crawling bond info...')

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
		empty_header = ['債券代碼', '債券簡稱', '發行人', '發行日期',
						'到期日期', '發行期限', '發行總面額', '債券網址']
		empty_df = pd.DataFrame(columns=empty_header)
		empty_count = 0
		# Create invalid bond DataFrame for URLs that cannot be connected
		invalid_header = ['債券代碼', '債券簡稱', '發行人', '發行日期',
						  '到期日期', '發行期限', '發行總面額', '債券網址']
		invalid_df = pd.DataFrame(columns=invalid_header)
		invalid_count = 0

		# Crawl bond info from each bond URL
		for bondName, bondURL in zip(bond_list_df['Short Name'], bond_list_df['Bond Database']):
			# Retry when ConnectionError happened
			connect_count = 0
			while connect_count < max_attempt:
				time.sleep(timeout)
				try:
					bond_page = getWebpage(bondURL)
				except Exception as e:
					print('---------------------------------------------------')
					print('Crawling failed with error:', str(e))
					if connect_count == max_attempt-1:
						print('Failed:', bondName)
						print('Reached maximum number of attempts. Moving to next item.')
						# Save invalid bond info
						invalid_df.loc[invalid_count] = bond_list_df.loc[bond_count+empty_count+invalid_count]
						invalid_count += 1
						break
					else:
						print('Reconnect:', bondName)
					print('---------------------------------------------------')
					connect_count += 1
					continue

				if bond_page:
					print('Crawling:', bondName)
					try:
						soup = BeautifulSoup(bond_page, 'html.parser')
						rows = soup.find('table', 'hasBorder').find_all('tr')
						for index, row in enumerate(rows):
							if index == 0:
								bond_yrn = [int(s) for s in row.find_all('td')[0].text.split() if s.isdigit()][0]
								offering_method = row.find_all('td')[1].text.split('：')[1].strip()
							elif index == 1:
								application_date = transferDate(row.find_all('td')[0].text.split('：')[1])
								approved_date = transferDate(row.find_all('td')[1].text.split('：')[1])
							elif index == 2:
								issued_date = transferDate(row.find_all('td')[0].text.split('：')[1])
								# Use year as unit of bond period
								bond_period = [float(s) for s in re.split(' |：|年', row.find_all('td')[1].text) if s.isdigit()]
								bond_period = bond_period[0] + round(bond_period[1]/12, 1)
							elif index == 3:
								delivery_date = transferDate(row.find_all('td')[0].text.split('：')[1])
								delisted_date = transferDate(row.find_all('td')[1].text.split('：')[1])
							elif index == 4:
								shareholder_date = transferDate(row.find_all('td')[0].text.split('：')[1])
							elif index == 5:
								listed_type = row.find_all('td')[0].text.split('：')[1].strip()
								listed_location = row.find_all('td')[1].text.split('：')[1].strip()
							elif index == 6:
								listed_date = transferDate(row.find_all('td')[0].text.split('：')[1])
								issued_currency = row.find_all('td')[1].text.split()[0].split('：')[1]
								issued_exchange_rate = float(row.find_all('td')[1].text.split()[-1])
							elif index == 7:
								bond_id = row.find_all('td')[0].text.split('：')[1]
								bond_abbrev = row.find_all('td')[1].text.split('：')[1].strip()
							elif index == 8:
								bond_chi_name = row.find_all('td')[0].text.split('：')[1].strip()
								issuer = row.find_all('td')[1].text.split('：')[1].strip()
							elif index == 9:
								bond_eng_name = row.find_all('td')[0].text.split('：')[1].strip()
							elif index == 10:
								price_reset_date = transferDate(row.find_all('td')[0].text.split('：')[1].split('民國')[1].strip())
								next_price_reset_date = transferDate(row.find_all('td')[1].text.split('：')[1].split('民國')[1].strip())
							elif index == 11:
								CFI = row.find_all('td')[0].text.split('：')[1]
								ISIN = row.find_all('td')[1].text.split('：')[1]
							elif index == 12:
								applied_issued_amount = int(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', ''))
							elif index == 13:
								actual_issued_amount = int(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', ''))
							elif index == 14:
								transfered_bond = int(row.find_all('td')[0].text.split('：')[1].split('張')[0].replace(',', ''))
							elif index == 15:
								transfered_entitled_cert = int(row.find_all('td')[0].text.split('：')[1].split('股')[0].replace(',', ''))
							elif index == 16:
								transfered_stock = int(row.find_all('td')[0].text.split('：')[1].split('股')[0].replace(',', ''))
							elif index == 17:
								repaid_amount = int(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', ''))
							elif index == 18:
								buyback_bond = int(row.find_all('td')[0].text.split('：')[1].split('張')[0].replace(',', ''))
							elif index == 19:
								sold_bond = int(row.find_all('td')[0].text.split('：')[1].split('張')[0].replace(',', ''))
							elif index == 20:
								remaining_issued_amount = int(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', ''))
							elif index == 21:
								repay_ontime = row.find_all('td')[0].text.split('，')[0].split('：')[1]
								repay_process_detail = row.find_all('td')[0].text.split('，')[1].split('：')[1].strip()
							elif index == 22:
								breach_penalty = int(row.find_all('td')[0].text.split()[1].replace(',', ''))
								penalty_result = row.find_all('td')[0].text.split()[-1].split('：')[-1].strip()
							elif index == 23:
								par_value = int(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', ''))
								issued_number = int(row.find_all('td')[1].text.split('：')[1].split('張')[0].replace(',', ''))
							elif index == 24:
								issued_price = float(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', ''))
							elif index == 25:
								coupon_rate = float(row.find_all('td')[0].text.split('：')[1].split('%')[0])
								exdividend_date = row.find_all('td')[1].text.split('：')[1]
							elif index == 26:
								issued_conversion_price = float(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', ''))
								convertible_premium_rate = float(row.find_all('td')[1].text.split('：')[1].split('%')[0])
							elif index == 27:
								conversion_start_date = transferDate(row.find_all('td')[0].text.split('：')[1].split('～')[0])
								conversion_end_date = transferDate(row.find_all('td')[0].text.split('：')[1].split('～')[1])
								conversion_exchange_rate = float(row.find_all('td')[1].text.split('：')[1])
							elif index == 28:
								latest_conversion_price = float(row.find_all('td')[0].text.split('：')[1].split('元')[0].replace(',', ''))
								latest_convert_date = transferDate(row.find_all('td')[1].text.split('：')[1])
							elif index == 29:
								dividend_table = row.find_all('td')[0].text.split('：')[1]
								# Retrieve download link if table name is shown
								if dividend_table:
									dividend_table = TWSE_URL + row.find_all('td')[0].find('a', href=True)['href']
							elif index == 30:
								secured_detail = row.find_all('td')[0].text.split('：')[1].strip()
							elif index == 31:
								repay_detail = row.find_all('td')[0].text.split('：')[1].strip()
							elif index == 32:
								sell_condition = row.find_all('td')[0].text.split('：')[1].strip()
							elif index == 33:
								sell_earnings_rate = float(row.find_all('td')[0].text.split('：')[1].split('%')[0])
							elif index == 34:
								buyback_condition = row.find_all('td')[0].text.split('：')[1].strip()
							elif index == 35:
								buyback_earnings_rate = float(row.find_all('td')[0].text.split('：')[1].split('%')[0])
							elif index == 36:
								next_sell_date = transferDate(row.find_all('td')[0].text.split('：')[1])
								next_sell_price = float(row.find_all('td')[1].text.split('：')[1].split('%')[0])
							elif index == 37:
								underwriting_institute = row.find_all('td')[0].text.split('：')[1].strip()
							elif index == 38:
								consignee = row.find_all('td')[0].text.split('：')[1].strip()
								attest_institute = row.find_all('td')[1].text.split('：')[1].strip()
							elif index == 39:
								transfer_institute = row.find_all('td')[0].text.split('：')[1].strip()
								credit_rating = row.find_all('td')[1].text.split('：')[1].strip()
							elif index == 40:
								issued_change_date = transferDate(row.find_all('td')[0].text.split('：')[1])
							elif index == 41:
								issued_change_reason = row.find_all('td')[0].text.split('：')[1].strip()
							elif index == 42:
								restriction = row.find_all('td')[0].text.split('：')[1].strip()
							elif index == 43:
								remark = row.find_all('td')[0].text.split('：')[1].strip()
							elif index == 44:
								caller = row.find_all('td')[0].text.split('：')[1].strip()
							elif index == len(rows)-1:
								regulation = TWSE_URL + row.find_all('td')[0].find('a', href=True)['href']

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
						# Save bond info of empty URLs
						empty_df.loc[empty_count] = bond_list_df.loc[bond_count+empty_count+invalid_count]
						empty_count += 1
				else:
					print('---------------------------------------------------')
					print('Failed:', bondName)
					print('The bond page is connected without required information.')
					print('---------------------------------------------------')
					# Save bond info of empty URLs
					empty_df.loc[empty_count] = bond_list_df.loc[bond_count+empty_count+invalid_count]
					empty_count += 1
				break

		# Write bond info to CSV sorted by bond ID
		bond_df.sort_values(by=['債券代碼'], ascending=True, inplace=True)
		bond_df.to_csv(os.path.join(info_path, today_datetime + '.csv'), encoding='utf_8_sig', index=False)

		# Write empty bond info to CSV sorted by bond ID if any
		if not empty_df.empty:
			empty_df.sort_values(by=['債券代碼'], ascending=True, inplace=True)
			empty_df.to_csv(os.path.join(info_path, today_datetime + '_empty.csv'), encoding='utf_8_sig', index=False)

		# Write invalid bond info to CSV sorted by bond ID if any
		if not invalid_df.empty:
			invalid_df.sort_values(by=['債券代碼'], ascending=True, inplace=True)
			invalid_df.to_csv(os.path.join(info_path, today_datetime + '_invalid.csv'), encoding='utf_8_sig', index=False)

		print('===================================================')
		print('Crawling finished.')
		print('Number of bonds retrieved: %d' % (len(bond_df.index)))
		print('Number of empty bonds: %d' % (len(empty_df.index)))
		print('Number of invalid bonds: %d' % (len(invalid_df.index)))
		break
	end_time = datetime.now()
	time_diff = end_time - start_time
	print('Execution time:', str(time_diff))
	print('===================================================')


if __name__ == '__main__':
	main()
