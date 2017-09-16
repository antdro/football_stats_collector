# football_stats_collector
Collects football stats across 143 countries and provides with trading signals

async_finder.py
	- collects players statistics and match results for premier league , championship, serie a, la liga, bundesliga and ligue 1
	- data is collected from https://uk.sports.yahoo.com/
	- data is stored in matches.h5 and players.h5 files


finder.py
	- collects match results for leagues from 143 countries
	- data is collected from http://www.scorespro.com/soccer/
	- data is stored in history.h5


visualiser.ipynb
	- reports current means of selected markets in comparison to its expected mean
	- information provided by a report might serve as a signal for trading
	

chromedriver
	- standalone server used by selenium in finder.py


matches.h5
	- database template for match results collected by async_finder.py and used by report_builder.ipynb


players.h5
	- database template for players statistics collected by async_finder.py


history.h5
	- database template for match results collected by finder.py


links.txt
	- links for databases to download
