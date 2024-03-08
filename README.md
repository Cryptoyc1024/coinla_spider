# Get real-time cryptocurrency quotes

ccy_daily.py

Currency daily market data crawler for summarizing data

chart.py

Currency market chart crawler, crawling market data every few minutes and every day
It also supports calculation mode, which allows you to calculate the currencies that do not have data on the day.

concept.py

Currency section concept crawler, crawls the latest sections and associates currencies

currency.py

Currency crawler, crawling and updating currency basic information,
And calculate some statistical data such as currency circulation value and market value by yourself

develop.py

Currency development progress crawler, mainly github submission time and community interaction and other data

event.py

Currency event crawler, crawling currency event time nodes and descriptions

exchange.py

Exchange crawler, crawling exchange information and trading pair data

exrate.py

Legal currency exchange rate crawler, which stores exchange rates and is used on the front and back ends to convert market price data into multiple legal currencies.

holder.py

Crawl mainstream block explorers

news.py

Crawl mainstream media news

notice.py

Mainstream exchange announcement crawler

otc_price.py

OTC OTC price crawler for mainstream currencies

calc_cqn.py

Trading pair market data calculation, real-time calculation of the latest network-wide price, trading volume and other data for all trading pairs
There is no crawler behavior, just for the sake of uniformity, Scrapy is used to run the call.

total_value.py

Crawler of total currency market capitalization
