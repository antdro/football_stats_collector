# Python 3.6

import re
import pandas as pd
from pandas import HDFStore
import time
from bs4 import BeautifulSoup as BS
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit, quote
from tornado import gen, ioloop, queues, httpclient
import warnings

warnings.filterwarnings("ignore")

sport = 'football'
leagues = { 'premier-league' : 38, 
			'championship' : 46,
			'serie-a' : 38,
			'la-liga' : 38,
			'bundesliga' : 34,
			'ligue-1' : 38
          }

h5_path = {
			'premier-league' : 'england/premier-league/2017', 
			'championship' : 'england/championship/2017',
			'serie-a' : 'italy/serie-a/2017',
			'la-liga' : 'spain/la-liga/2017',
			'bundesliga' : 'germany/bundesliga/2017',
			'ligue-1' : 'france/ligue-1/2017'	
}

positions = [ 'Substitutes', 'Goalkeepers', 'Defenders', 'Midfielders', 'Forwards']
fields = ['home', 'away']

request_url = '/fixtures/?schedState=2&dateRange='
base_url = 'https://uk.sports.yahoo.com/'

def encode_non_ascii_url(url):

    url_split_result = urlsplit(url)
    url_list = list(url_split_result)
    url_path = url_list[2]

    url_list[2] = quote(url_path)
    url = urlunsplit(url_list)
    
    return url


def get_fixture_from_bs(bs):

	team = {}
	home_team = {}
	away_team = {}
	fixture = {}

	counter = 0
	for table in bs.find_all('table'):

		position_headers = []

		for thead in table('thead'):
			for th in thead('th'):
				position_headers.append(th.text.strip())

		if len(set(position_headers).intersection(positions)):

			for tbody in table('tbody'):

				players = []
				for tr in tbody.find_all('tr'):
					try:
						player = (tr.find('a').attrs['title'])
						team[position_headers[0]] = player
					except:
						pass

					stats = []
					for td in tr.find_all('td'):

						if td.text == '-':
							stats.append('0')
						else:
							stats.append(td.text)

					player_stats = dict(zip(position_headers[1:], stats))
					players.append({player : player_stats})

				team[position_headers[0]] = players

		if counter == 0:
			if 'Substitutes' in position_headers:
				home_team = team 
				team = {}
				counter += 1

	away_team = team
	fixture['home'] = home_team
	fixture['away'] = away_team
    
	return fixture


def get_teams_from_bs(bs):
    
	h3_list = []
	teams = {}
    
	for h3 in bs.find_all('h3'):
		h3_list.append(h3.text)
        
	teams['home'] = h3_list[2]
	teams['away'] = h3_list[3]
    
	return teams


def get_kickoff_from_bs(bs):

	try:
		pattern = "(.*?)\| (\d\d\d\d-\d\d-\d\d) \|(.*?)"
		date = re.search(pattern, bs.find('title').text).group(2)
	except Exception as e:
		print (e)
    
	return date


def build_fixture_df(fixture, teams, kickoff):

	stats = []
	for field in fields:
		for position in positions:
			players = pd.DataFrame(fixture).loc[position][field]
			for player in players:
				player_info = { 'position' : position[:-1],
								'team' : teams[field], 
								'field' : field, 
								'opponent' : teams[list(set(fields).difference([field]))[0]],
								'kickoff' : kickoff}
				list(player.values())[0].update(player_info)
				stats.append(player)

	fixture_df = pd.DataFrame()
	for player in stats:
		fixture_df = pd.concat([fixture_df, pd.DataFrame(player).T], axis = 0)

	fixture_df.fillna(value = 0, inplace = True)
	fixture_df['player'] = list(fixture_df.index)
	#fixture_df.reset_index(drop = True, inplace = True)

	fixture_df = fixture_df.loc[:, ['player', 'G', 'GA', 'S', 'FC', 'FW', 'GC', 'SAV', 
									'team', 'position', 'field', 'opponent', 'kickoff']]
    
	return fixture_df


def get_match_results(title):

    matches = {}

    teams = title.split(':')[0].split('-')

    home = teams[0]
    away = teams[1]
    
    goals = re.search(('\d - \d'), title)[0]

    home_goals = goals.split(' - ')[0]
    away_goals = goals.split(' - ')[1]

    home_team = home[:-3].strip()
    away_team = away[3:].strip()

    matches['home'] = home_team
    matches['away'] = away_team
    matches['FTHG'] = int(home_goals)
    matches['FTAG'] = int(away_goals)
    
    matches_df = pd.DataFrame([matches])
    matches_df = matches_df.loc[:, ['home', 'away', 'FTHG', 'FTAG']]
    
    return matches_df


def update_matches(match_results_df, league):
	
	h5 = HDFStore('matches.h5')
	h5[h5_path[league]] = match_results_df
	h5.close()

def update_players(league_stats_df, league):
	
	h5 = HDFStore('players.h5')
	h5[h5_path[league]] = league_stats_df
	h5.close()


@gen.coroutine
def main():

	@gen.coroutine
	def get_bs_from_url(url):

		while True:
			
			try:			
				response = yield httpclient.AsyncHTTPClient().fetch(url)
				html = response.body if isinstance(response.body, str) else response.body.decode()				
			except URLError as e:
				if hasattr(e, 'reason'):
					print('URLError. Failed to reach a server.')
					print('Reason: ', e.reason)
				elif hasattr(e, 'code'):
					print('URLError. The server couldn\'t fulfill the request.')
					print('Error code: ', e.code)
				continue
			except HTTPError as e:
				if hasattr(e, 'reason'):
					print('HTTPError. Reason: ', e.reason)
				elif hasattr(e, 'code'):
					print('HTTPError. Error code: ', e.code)
				continue
			except Exception as e: 
				print (e.message)

			break

		bs4 = BS(html, "lxml")

		return bs4


	@gen.coroutine
	def get_urls_for_round(sport, league, _round):
    
		round_url = base_url + sport + '/' + league + request_url
		sport_league_url = '/' + sport + '/' + league
	    
		bs = yield get_bs_from_url(round_url + str(_round))
		urls = []
		
		for tag in bs.find_all('a', href=True):
			if (sport_league_url in tag['href']) & ('yahoo.com' not in tag['href']):
				urls.append(base_url + tag['href'])
		
		return urls


	@gen.coroutine
	def get_stats_for_round(urls_for_round):
		
		for url in urls_for_round:

			url = encode_non_ascii_url(url)
			bs = yield get_bs_from_url(url)
			title = bs.find_all('title')[0].text

			print ('collecting stats for {}'.format(url))
			if 'Finished' in title:
				matches_df = get_match_results(title)
				fixture = get_fixture_from_bs(bs)
				teams = get_teams_from_bs(bs)
				kickoff = get_kickoff_from_bs(bs)
				fixture_df = build_fixture_df(fixture, teams, kickoff)
				fixtures_list.append(fixture_df)
				matches_df['kickoff'] = kickoff
				matches_list.append(matches_df)


	@gen.coroutine
	def round_handler():
		while True:
			try:
				_round = yield q.get()
				if _round not in processing:

					processing.append(_round)
					print ('doing round {}'.format(_round))
					urls_for_round = yield get_urls_for_round(sport, league, _round)
					yield get_stats_for_round(urls_for_round)
					q.task_done()

			except Exception as e:
				print (e)

	@gen.coroutine
	def next_round_finder():
		while True:
			try:
				_round = yield q_round.get()

				if not next_round_list:
					if _round not in processing_next_round:

						processing_next_round.append(_round)
						urls_for_round = yield get_urls_for_round(sport, league, _round)
						
						last_url = urls_for_round[-1:][0]
						last_url = encode_non_ascii_url(last_url)
						bs = yield get_bs_from_url(last_url)

						if not 'Finished' in bs.find_all('title')[0].text:
							next_round_list.append(_round)

						q_round.task_done()
				else:
					q_round.task_done()

			except Exception as e:
				print ('next_round_finder()')
				print (e)


	for league in leagues:

		print (league)

		total_number_of_rounds = leagues[league]
		number_of_next_round_finders = 3

		# find next noncomplete league round
		q_round = queues.Queue()
		processing_next_round = []
		next_round_list = []

		for _round in range(1, total_number_of_rounds):
			q_round.put(_round)

		for _ in range(1, number_of_next_round_finders):
			next_round_finder()

		yield q_round.join()
		next_round = min(next_round_list)

		# collect stats for league
		q = queues.Queue()
		league_stats_df = pd.DataFrame()
		match_results_df = pd.DataFrame()
		processing = []
		fixtures_list = []
		matches_list = []

		for next_round_handler in range(1, next_round):
			print ('round_handler {} created'.format(next_round_handler))
			round_handler()

		for _round in range(1, next_round):
			q.put(_round)

		yield q.join()

		for fixture in fixtures_list:
			league_stats_df = pd.concat([league_stats_df, fixture])		

		for match_df in matches_list:
			match_results_df = pd.concat([match_results_df, match_df])

		league_stats_df.sort_values(by = ['kickoff', 'team', 'opponent', 'position'], inplace = True)		
		number_of_records = league_stats_df.shape[0]
		league_stats_df.index = range(1, number_of_records + 1)
		update_players(league_stats_df, league)

		match_results_df.sort_values(by = ['kickoff'], inplace = True)
		number_of_records = match_results_df.shape[0]
		match_results_df.index = range(1, number_of_records + 1)
		update_matches(match_results_df, league)


if __name__ == '__main__':
	import logging
	logging.basicConfig()
	io_loop = ioloop.IOLoop.current()
	io_loop.run_sync(main)