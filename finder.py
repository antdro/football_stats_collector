from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import pandas as pd
from bs4 import BeautifulSoup as bs
import datetime
import re
from selenium import webdriver
import os
import time
import json

kickoff_pattern = "\d\d-\d\d-\d\d\d\d"
base_url = 'http://www.scorespro.com/soccer/'
years = range(1993, 2018)

with open('countries.json') as file:
    countries = json.load(file)['countries']

def from_url_to_bs4(url):

    while True:
        req = Request(url)
        try:
            response = urlopen(req)
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
        break

    html_read = response.read()
    bs4 = bs(html_read, "lxml")

    return bs4


def fetch_fixtures(html):

    bs4 = bs(html, 'lxml')
    
    fixture = []
    fixtures = []
    for (n, table) in enumerate(bs4.find_all('table')):
        if n > 0:
            for tbody in table('tbody'):
                for tr in tbody.find_all('tr'):
                    fixture = []
                    for td in tr.find_all('td'):
                        for a in td.find_all('a'):
                            result = re.search(kickoff_pattern, str(a.attrs['href']))
                            if result:
                                date = result.group(0)
                                fixture.append(date)
                            fixture.append(a.text)
                fixtures.append(fixture)
    
    league = pd.DataFrame(fixtures)
    league.columns = ['home', 'kickoff', 'FT', 'away']
    
    try: 
        scores = pd.DataFrame([score.split(' - ') for score in league.FT if score])
    except AttributeError:
        try:
            scores = pd.DataFrame([score.split('-') for score in league.FT if score])
        except Exception as e:
            pass
    
    scores.columns = ['FTHG', 'FTAG']
    league = pd.concat([league, scores], axis = 1)
    league = league.loc[:, ['home', 'away', 'FTHG', 'FTAG', 'kickoff']]
    
    return league


def find_number_of_games(url):
    
    pattern = '/(\d+)››'
    bs4 = from_url_to_bs4(url)
    for (n, table) in enumerate(bs4.find_all('table')):
            for tbody in table('tbody'):
                for tr in tbody.find_all('tr'):
                    for td in tr.find_all('td'):
                        if n == 0:
                            result = re.search(pattern, td.text)
                            if result:
                                number_of_games = int(result.group(1))
                            else:
                                number_of_games = 50			  

    return number_of_games


def fetch_league(url, file_path):
    
    chrome_path = os.getcwd()
    driver = webdriver.Chrome(chrome_path + '/chromedriver')

    driver.get(url)
    html = driver.page_source
    fixtures_df = fetch_fixtures(html)

    x_path = '''//*[@id="national"]/table/tbody/tr/td[2]/a/span'''
    counter = 0
    search = True

    number_of_games = find_number_of_games(url)
    number_of_clicks = number_of_games // 50

    while counter < number_of_clicks:
        try:
            element = driver.find_element_by_xpath(x_path)
            element.click()
            time.sleep(5)
            html = driver.page_source
            fixture_df = fetch_fixtures(html)
            fixtures_df = pd.concat([fixtures_df, fixture_df])
            x_path = '''//*[@id="national"]/table/tbody/tr/td[2]/a[2]/span'''
        except:
            time.sleep(5)
            html = driver.page_source
            fixture_df = fetch_fixtures(html)
            fixtures_df = pd.concat([fixtures_df, fixture_df])
        counter += 1

    driver.close()

    fixtures_df.dropna(axis = 0, how = 'any', inplace = True)    
    fixtures_df['kickoff'] = \
    fixtures_df['kickoff'].apply(lambda date: int(datetime.datetime.strptime(date, '%d-%m-%Y').strftime("%s")))

    fixtures_df.sort_values(by = 'kickoff', ascending = True,  inplace = True)
    fixtures_df.reset_index(drop = True, inplace = True)

    fixtures_df['kickoff'] = \
    fixtures_df['kickoff'].apply(lambda date: datetime.datetime.fromtimestamp(date).strftime('%d-%m-%Y'))

    fixtures_df.to_csv(file_path + '.csv', encoding = 'latin-1')


def load_folder_into_h5(h5_file_name, folder_path):
    
    '''
    Populate h5 database with all leagues stores as csv files in folder_path
    '''

    for subdir, dirs, files in os.walk(folder_path):
        for file in files:

            file_path = os.path.join(subdir, file)
            h5_path = file_path.replace(folder_path, '').replace('.csv', '')

            df = pd.read_csv(file_path, encoding = 'latin-1')
            df = df.iloc[:, 1:]
            df.dropna(axis = 0, inplace = True)

            try:
                h5 = h5py.File(h5_file_name, 'r+')
                h5.create_group(h5_path)
                h5.close()
            except :
                pass

            try:
                store = HDFStore(h5_file_name)
                store[h5_path] = df
                store.close()
            except :
                pass
    
    
def create_directory(name):
    if not os.path.exists(name):
        os.makedirs(name)
          
    
for country in countries:
    
    country_path = 'data/' + country
    create_directory(country_path)
    
    country_url = base_url + country + '/'
    bs4 = from_url_to_bs4(country_url)

    leagues = []
    for div in bs4.find_all('div'):
        for select in div.find_all('select'):
            for option in select.find_all('option'):
                league = option.attrs['value']
                if league != '0' and 'cup' not in league:
                    leagues.append(league)
    leagues = list(set(leagues))

    for league in leagues:
        
        league_path = country_path + '/' + league
        create_directory(league_path)
        
        league_url = country_url + league + '/'
        for year in years:
            
            year_path = league_path + '/' + str(year)
            year_url = league_url + '/' + str(year) + '-' + str(year + 1) + '/results'
            
            print (year_url)
            print (year_path)
            
            try:
                fetch_league(year_url, year_path)
            except Exception as e:
                print (e)
                try:
                    year_url = league_url + '/' + str(year) + '/results'
                    print (year_url)
                    fetch_league(year_url, year_path)
                except:
                    print (e)
                    pass


load_folder_into_h5('history.h5', 'data')
