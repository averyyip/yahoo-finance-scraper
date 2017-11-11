import sys
import pandas as pd
from bs4 import BeautifulSoup
import urllib.request
import re
import os
from multiprocessing import Pool
import datetime
import time

output_file_name = "scraped_yahoo_tickers.csv"

def scrape_html(ticker):
	stock_series = pd.Series()
	html = "https://finance.yahoo.com/quote/" + ticker + "/key-statistics?p=" + ticker
	content = urllib.request.urlopen(html).read()
	
	#Loading to BS4
	soup = BeautifulSoup(BeautifulSoup(content).encode("utf-8"))

	if re.search(r"All (\d)", soup.prettify()):
		return
	try:
		stats_table = soup.find("section", {"data-test":"qsp-statistics"})
		rows = stats_table.find_all("tr")
	except:
		print(ticker + " does not exist on Yahoo Finance")
		return

	#Current Price and Ticker
	try:
		price_html = urllib.request.urlopen("https://finance.yahoo.com/quote/" + ticker + "?p=" + ticker)
		price = BeautifulSoup(price_html).find("span", {"class":"Trsdu(0.3s)"}).text
	except:
		price = "N/A"
		print(ticker + " price does not exist")
	stock_series["price"] = price
	stock_series["ticker"] = ticker

	#Scrape all the data from the statistics table
	for row in rows:
		row_data = [data.text for data in row.find_all("td")]
		cleaned_key = clean_string(row_data[0])
		cleaned_val = clean_value(row_data[1])
		stock_series[cleaned_key] = cleaned_val

	# Turn into csvs as a string
	values = series_to_str(stock_series)
	with open(output_file_name, "a+") as file:
		file.write(values)
	return stock_series

def clean_string(text):
	return re.sub(re.compile(r"\s+\d$"), "", text).strip().lower()

#NaNs to 0s?
def clean_value(value):
	if re.search(r",", value):
		return '"' + value.strip() + '"'
	elif re.search(r"\dB", value):
		return "{0:.2f}".format(float(value.strip("B"))*1000)+"M"
	return value.strip()

def series_to_str(series):
	ticker_data = []

	dt = datetime.datetime.now()
	date = str(dt.month) + "/" + str(dt.day) + "/" + str(dt.year)

	ticker_data.append(date)
	ticker_data.append(series["price"])
	ticker_data.append(series["52 week high"])
	ticker_data.append(series["52 week low"])
	ticker_data.append(series["peg ratio (5 yr expected)"])
	ticker_data.append(series["market cap (intraday)"])
	ticker_data.append(series["shares outstanding"])
	ticker_data.append(series["avg vol (3 month)"])
	ticker_data.append(series["trailing annual dividend yield"])
	ticker_data.append(series["diluted eps (ttm)"])
	ticker_data.append(series["forward p/e"])
	ticker_data.append(series["revenue (ttm)"])
	ticker_data.append(series["total cash per share (mrq)"])
	ticker_data.append(series["total debt (mrq)"])
	ticker_data.append(series["book value per share (mrq)"])
	ticker_data.append(series["% held by insiders"])
	ticker_data.append(series["% held by institutions"])
	ticker_data.append(series["profit margin"])
	ticker_data.append(series["operating margin (ttm)"])
	ticker_data.append(series["52-week change"])
	ticker_data.append(series["s&p500 52-week change"])
	ticker_data.append(series["ticker"])
	return ",".join(ticker_data) + "\n"
	
def run():
	#Grabs ticker file path from command line input
	ticker_path = sys.argv[1]

	#If ticklist file is missing, tell user to put it
	try:
		if len(sys.argv) == 3 and sys.argv[2].lower() == "true":
			with open(output_file_name, "a+") as file:
				file.write(",".join(columns)+"\n")
	except:
		print("Don't forget to put in the name of the ticklist file")

	#Assuming tickers are split by new line
	tickers = open(ticker_path).read().split("\n")

	pool = Pool(5)
	pool.map(scrape_html, tickers)

if __name__ == "__main__":
	run()



