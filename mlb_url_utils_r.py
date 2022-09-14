#!/usr/bin/env python

"""This module gets the XML data that other functions use.
It checks if the data is cached first, and if not,
gets the data from mlb.com.
"""
from bs4 import BeautifulSoup as bs

import os
import pybaseball as pb
import statsapi as sa
from datetime import datetime as dt
from datetime import timedelta as tdlt
from time import sleep
from collections import Counter as cntr
from dateutil.parser import parse as dp

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait as wdw
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

try:
    from urllib.request import urlopen
    from urllib.error import HTTPError
except ImportError:
    from urllib2 import urlopen, HTTPError

team_dict = {}
# Templates For URLS
BASE_URL = ('http://gd2.mlb.com/components/game/mlb/'
            'year_{0}/month_{1:02d}/day_{2:02d}/')
GAME_URL = BASE_URL + 'gid_{3}/{4}'
PROPERTY_URL = 'http://mlb.mlb.com/properties/mlb_properties.xml'
ROSTER_URL = 'http://mlb.mlb.com/lookup/json/named.roster_40.bam?team_id={0}'
INJURY_URL = 'http://mlb.mlb.com/fantasylookup/json/named.wsfb_news_injury.bam'
STANDINGS_URL = ('http://mlb.mlb.com/lookup/json/named.'
                 'standings_schedule_date.bam?season={0}&'
                 'schedule_game_date.game_date=%27{1}%27&sit_code='
                 '%27h0%27&league_id=103&'
                 'league_id=104&all_star_sw=%27N%27&version=2')
STANDINGS_HISTORICAL_URL = ('http://mlb.mlb.com/lookup/json/'
                            'named.historical_standings_schedule_date.bam?'
                            'season={0}&game_date=%27{1}%27&sit_code=%27h0%27&'
                            'league_id=103&league_id=104&all_star_sw=%27N%27&'
                            'version=48')
IMPORTANT_DATES = ('http://lookup-service-prod.mlb.com/named.org_history.bam?'
                   'org_id=1&season={0}')
BCAST_INFO = ('http://mlb.mlb.com/lookup/json/named.mlb_broadcast_info.bam?'
              'team_id={}&season={}')
INNINGS_URL = BASE_URL + 'gid_{3}/inning/inning_all.xml'
# Local Directory
PWD = os.path.join(os.path.dirname(__file__))


def get_broadcast_info(team_id, year):
    try:
        return urlopen(BCAST_INFO.format(team_id, year))
    except HTTPError:
        raise ValueError('Failed to retrieve MLB broadcast information.')


def get_important_dates(year):
    try:
        return urlopen(IMPORTANT_DATES.format(year))
    except HTTPError:
        raise ValueError('Failed to retrieve MLB important dates information.')


def get_scoreboard(year, month, day):
    """Return the game file for a certain day matching certain criteria."""
    try:
        data = urlopen(BASE_URL.format(year, month, day) + 'scoreboard.xml')
    except HTTPError:
        data = os.path.join(PWD, 'default.xml')
    return data


def get_box_score(game_id):
    """Return the box score file of a game with matching id."""
    year, month, day = get_date_from_game_id(game_id)
    try:
        return urlopen(GAME_URL.format(year, month, day, game_id,
                                       'boxscore.xml'))
    except HTTPError:
        raise ValueError('Could not find a game with that id.')


def get_raw_box_score(game_id):
    """Return the raw box score file of a game with matching id."""
    year, month, day = get_date_from_game_id(game_id)
    try:
        return urlopen(GAME_URL.format(year, month, day, game_id,
                                       'rawboxscore.xml'))
    except HTTPError:
        raise ValueError('Could not find a game with that id.')


def get_game_events(game_id):
    """Return the game events file of a game with matching id."""
    year, month, day = get_date_from_game_id(game_id)
    try:
        return urlopen(GAME_URL.format(year, month, day, game_id,
                                       'game_events.xml'))
    except HTTPError:
        raise ValueError('Could not find a game with that id.')


def get_innings(game_id):
    """Return the innings file of a game with matching id."""
    year, month, day = get_date_from_game_id(game_id)
    try:
        return urlopen(INNINGS_URL.format(year, month, day, game_id))
    except HTTPError:
        raise ValueError('Could not find a game with that id.')


def get_overview(game_id):
    """Return the linescore file of a game with matching id."""
    year, month, day = get_date_from_game_id(game_id)
    try:
        return urlopen(GAME_URL.format(year, month, day, game_id,
                                       'linescore.xml'))
    except HTTPError:
        raise ValueError('Could not find a game with that id.')


def get_players(game_id):
    """Return the players file of a game with matching id."""
    year, month, day = get_date_from_game_id(game_id)
    try:
        return urlopen(GAME_URL.format(year, month, day, game_id,
                                       'players.xml'))
    except HTTPError:
        raise ValueError('Could not find a game with that id.')


def get_properties():
    """Return the current mlb properties file"""
    try:
        return urlopen(PROPERTY_URL)
    # in case mlb.com depricates this functionality
    except HTTPError:
        raise ValueError('Could not find the properties file. '
                         'mlb.com does not provide the file that '
                         'mlbgame needs to perform this operation.')


def get_roster(team_id):
    """Return the roster file of team with matching id."""
    try:
        return urlopen(ROSTER_URL.format(team_id))
    except HTTPError:
        raise ValueError('Could not find a roster for a team with that id.')


def get_standings(date):
    """Return the standings file for current standings (given current date)."""
    try:
        return urlopen(STANDINGS_URL.format(date.year,
                                            date.strftime('%Y/%m/%d')))
    except HTTPError:
        ValueError('Could not find the standings file. '
                   'mlb.com does not provide the file that '
                   'mlbgame needs to perform this operation.')


def get_historical_standings(date):
    """Return the historical standings file for specified date."""
    try:
        url = STANDINGS_HISTORICAL_URL.format(date.year,
                                              date.strftime('%Y/%m/%d'))
        return urlopen(url)
    except HTTPError:
        ValueError('Could not find standings for that date.')


def get_injuries():
    """Return the injuries file for specified date."""
    try:
        return urlopen(INJURY_URL)
    except HTTPError:
        ValueError('Could not find the injuries file. '
                   'mlb.com does not provide the file that '
                   'mlbgame needs to perform this operation.')


def get_date_from_game_id(game_id):
    year, month, day, _discard = game_id.split('_', 3)
    return int(year), int(month), int(day)

def statcast_stats(p = None, b = None, player = None):
	foo='bar'


def get_teams(season=None):
	import requests
	import json
	if season==None:
		season='''2022'''
	jdict = {}
	turl = """http://lookup-service-prod.mlb.com/json/named.team_all_season.bam?sport_code='mlb'&all_star_sw='N'&sort_order=name_asc&season='{0}'"""
	teams = requests.get(turl.format(season)).json()
	
	cteams = teams['team_all_season']['queryResults']['row']
	teamdict = {t['name_abbrev']:{'team_id':t['team_id'], 'team_name':t['name_display_long'], 'active_roster':{}, 'schedule':{}} for t in cteams}
	# for t in teamdict:
	# 	our_team = self.team(teamdict[t]['team_name'], teamdict[t]['team_id'], t)
	
	
	rurl = """https://statsapi.mlb.com/api/v1/teams/{0}/roster/{1}"""
	for t in teamdict:
		roster = sa.get('team_roster', {'teamId': teamdict[t]['team_id'], 'rosterType':'40Man'})
		# sleep(.5)
		print('getting roster for {0} with team id {1}'.format(str(t), str(teamdict[t]['team_id'])))
		print('var - roster.keys(): ' + ', '.join([*roster.keys()]))
		
		try:
			
			rl = roster['roster']
			
		except Exception:
			print('retrying roster request for {0} with team id {1}'.format(str(t), str(teamdict[t]['team_id'])))
			
			roster = sa.get('team_roster', {'teamId': teamdict[t]['team_id']})
			print('var - roster.keys(): ' + ', '.join([*roster.keys()]))
			
			rl = roster['roster']
		
		teamdict[t]['roster'] = {}
		teamdict[t]['roster']['pitchers'] = {}
		teamdict[t]['roster']['fielders'] = {}
		teamdict[t]['roster']['fielders'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position':i['position']['abbreviation'], 'stats':{'2022':{}, '2021':{}, '2020':{}}} for i in rl if i['position']['abbreviation'] != 'P'}
		teamdict[t]['roster']['pitchers'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position':i['position']['abbreviation'], 'stats':{'2022':{}, '2021':{}, '2020':{}}} for i in rl if i['position']['abbreviation'] == 'P'}
		tid = teamdict[t]['team_id']
		
		sched = sa.schedule(start_date='2022-04-07', end_date='2022-10-03', team=tid)
		sleep(.1)
		# sched_2021 = sa.schedule(start_date='2021-04-01', end_date='2021-10-03', team=tid)
		# sleep(.5)
		# sched_2020 = sa.schedule(start_date='2022-03-26', end_date='2022-09-27', team=tid)
		
		teamdict[t]['schedule'] = {s['game_date']:s for s in sched}
	# final_dict = get_stats(teamd = teamdict)	
	team_dict = teamdict.copy()
	
	for t in teamdict:
		p = teamdict[t]['roster']['pitchers']
		b = teamdict[t]['roster']['fielders']
		print('getting seasonal stats for {}'.format(t))
		for pl in p:
			
			plid = p[pl]['id']
			
			pparams22 = {'personIds': plid, 'hydrate': 'stats(group=[pitching],type=[season],season=2022)'}
			try:
				pl_stats_req = sa.get('people', pparams22)
				if 'people' not in pl_stats_req:
					continue
				else:
					if 'stats' in pl_stats_req['people'][0]:
						pl_stats = pl_stats_req['people'][0]['stats'][0]['splits'][0]['stat']
						teamdict[t]['roster']['pitchers'][plid]['stats']['2022'] = pl_stats
			#code
			except Exception:
				continue
			
			pparams21 = {'personIds': plid, 'hydrate': 'stats(group=[pitching],type=[season],season=2021)'}
			
			try:
				pl_stats_req = sa.get('people', pparams21)
				if 'people' not in pl_stats_req:
					continue
				else:
					if 'stats' in pl_stats_req['people'][0]:
						
						pl_stats = pl_stats_req['people'][0]['stats'][0]['splits'][0]['stat']
						teamdict[t]['roster']['pitchers'][plid]['stats']['2021'] = pl_stats
			except Exception:
				continue
			
			pparams20 = {'personIds': plid, 'hydrate': 'stats(group=[pitching],type=[season],season=2020)'}
			try:
				pl_stats_req = sa.get('people', pparams20)
				if 'people' not in pl_stats_req:
					continue
				else:
					if 'stats' in pl_stats_req['people'][0]:
						pl_stats = pl_stats_req['people'][0]['stats'][0]['splits'][0]['stat']
						teamdict[t]['roster']['pitchers'][plid]['stats']['2020'] = pl_stats
			except Exception:
				continue
			
			
		for pl in b:
			
			plid = b[pl]['id']
			
			bparams22 = {'personIds': plid, 'hydrate': 'stats(group=[batting],type=[season],season=2022)'}
			try:
				pl_stats_req = sa.get('people', bparams22)
				if 'people' not in pl_stats_req:
					continue
				else:
					if 'stats' in pl_stats_req['people'][0]:
						pl_stats = pl_stats_req['people'][0]['stats'][0]['splits'][0]['stat']
						teamdict[t]['roster']['fielders'][plid]['stats']['2022'] = pl_stats	
			except Exception:
				continue
		
			bparams21 = {'personIds': plid, 'hydrate': 'stats(group=[batting],type=[season],season=2021)'}
			try:
				pl_stats_req = sa.get('people', bparams21)
				if 'people' not in pl_stats_req:
					continue
				else:
					if 'stats' in pl_stats_req['people'][0]:
						pl_stats = pl_stats_req['people'][0]['stats'][0]['splits'][0]['stat']
						teamdict[t]['roster']['fielders'][plid]['stats']['2021'] = pl_stats
			except Exception:
				continue
				
			bparams20 = {'personIds': plid, 'hydrate': 'stats(group=[batting],type=[season],season=2020)'}
			try:
				pl_stats_req = sa.get('people', bparams20)
				if 'people' not in pl_stats_req:
					continue
				else:
					if 'stats' in pl_stats_req['people'][0]:
						pl_stats = pl_stats_req['people'][0]['stats'][0]['splits'][0]['stat']
						teamdict[t]['roster']['fielders'][plid]['stats']['2020'] = pl_stats	
			except Exception:
				continue
			
	
	return teamdict


def stat_counter(team_stats, **kwargs):
	resp_dict = {'batting':{}, 'pitching':{}}
	try:
		today = dt.today()
		before_today = []
		date_keys = [*team_stats.keys()]
		before_today = [d for d in date_keys if today > dp(d)]
		
	except Exception as e:
		print('Something went wrong, see the following error')
		print(repr(e))
		return False
	
	bt = before_today
	
	for d in bt:
		for b in team_stats[d]['batting']:
			if b not in resp_dict['batting']:
				resp_dict['batting'][b] = {}
				resp_dict['batting'][b] = team_stats[d]['batting'][b]
			else:
				cur = cntr(resp_dict['batting'][b])
				nxt = cntr(team_stats[d]['batting'][b])
				cur.update(nxt)
				resp_dict['batting'][b] = dict(cur)
		
		for p in team_stats[d]['pitching']:
			if p not in resp_dict['pitching']:
				resp_dict['pitching'][p] = {}
				resp_dict['pitching'][p] = team_stats[d]['pitching'][p]
			else:
				cur = cntr(resp_dict['pitching'][p])
				nxt = cntr(team_stats[d]['pitching'][p])
				cur.update(nxt)
				resp_dict['pitching'][p] = dict(cur)

	combined_stats = {'batting':resp_dict['batting'], 'pitching':resp_dict['pitching']}
	return combined_stats
	


def split_by_games(teams_list, td=team_dict.copy(), no_games=0, start_date=False, end_date=False):
	if no_games > 0:
		ng = no_games
	if start_date and end_date:
		sd = start_date
		ed = end_date
	team_game_list = {}
	tl = teams_list
	print(tl)
	od = {t:{'combined':{}, 'games':[]} for t in tl}
	team_game_list = {t:{d:{'batting':td[t]['schedule'][d]['stats']['batting'], 'pitching':td[t]['schedule'][d]['stats']['pitching']} for d in td[t]['schedule'] if 'stats' in td[t]['schedule'][d]} for t in tl}
	ct_stats = {}
	
	tgl = team_game_list
	
	for t in tgl:
		if t not in ct_stats:
			ct_stats[t] = {'batting':{}, 'pitching':{}}
		for g in tgl[t]:
			
			for pl in tgl[t][g]['batting']:
				if 'name' in tgl[t][g]['batting'][pl]:
					
					pname = tgl[t][g]['batting'][pl]['name']
					tgl[t][g]['batting'][pl].pop('name')
					ctyes = True
				if pl not in ct_stats[t]['batting']:
					ct_stats[t]['batting'][pl] = {}
					ct_stats[t]['batting'][pl] = tgl[t][g]['batting'][pl]
				else:
					tcount = cntr(ct_stats[t]['batting'][pl])
					tcount.update(tgl[t][g]['batting'][pl])
					ct_stats[t]['batting'][pl] = dict(tcount)
				if ctyes:
					ct_stats[t]['batting'][pl]['name'] = pname
					
			for pl in tgl[t][g]['pitching']:
				if 'name' in tgl[t][g]['pitching'][pl]:
					
					pname = tgl[t][g]['pitching'][pl]['name']
					tgl[t][g]['pitching'][pl].pop('name')
					ctyes = True
				if pl not in ct_stats[t]['pitching']:
					ct_stats[t]['pitching'][pl] = {}
					ct_stats[t]['pitching'][pl] = tgl[t][g]['pitching'][pl]
				else:
					#make counter object and update through the boxscore
					#using game date as container
					tcount = cntr(ct_stats[t]['pitching'][pl])
					tcount.update(tgl[t][g]['pitching'][pl])
					ct_stats[t]['pitching'][pl] = dict(tcount)
				if ctyes:
					ct_stats[t]['pitching'][pl]['name'] = pname	
	# tgl['totals'] = {}
	# tgl['totals'] = ct_stats
	reqd = {t:{d:td[t]['schedule'][d]['stats'] for d in td[t]['schedule'] } for t in tl}
	reql = {t:[td[t]['schedule'][d]['stats'] for d in td[t]['schedule']] for t in tl}
	
	return tgl, ct_stats, reql

def ssp(param_dict, stat_dict=team_dict):
	tids = {t:stat_dict[t]['team_id'] for t in stat_dict}
	tids |= {stat_dict[t]['team_id']:t for t in stat_dict}
	
	pd = param_dict
	if 'teams' not in pd:
		return False
	else:
		for t in pd['teams']:
			params = {}
			reqt = str(t)
			sd = False
			ed = False
			oppt = False
			stats = False
			if 'start_date' in pd['teams'][t]:
				sd = pd['teams'][t]['start_date']
				
			if 'end_date' in pd['teams'][t]:
				ed = pd['teams'][t]['end_date']
			else:
				ed = dt.today().strftime('%y-%m-%d')
				
			if 'opp' in pd['teams'][t]:
				oppt = tid[str(pd['teams'][t]['opp'])]
				
			if 'stats' not in pd['teams'][t]:
				stats = 'both'
			elif pd['teams'][t]['stats'] == 'both':
				stats = 'both'
			elif pd['teams'][t]['stats'] == 'batting':
				stats = 'batting'
			else:
				stats='pitching'
			
			if sd:
				params['sd'] = sd
			if ed:
				params['ed'] = ed
			
			if stats:
				params['stats'] = stats
			if oppt:
				params['oppt'] = oppt
			params['team'] = reqt
			
def stat_split(params):
	p = params
	sd = False
	ed = False
	if 'sd' in params:
		sd = params['sd']
	else:
		sd = '2022-03-01'
	if 'ed' in params:
		ed = params['ed']
	else:
		ed = dt.today().strftime('%Y-%m-%d')
	team = params['team']
	
	teamd  = team_dict[team]['schedule']
	
	teamg = {d:teamd[d] for d in teamd if (dp(d) > dp(sd) and do(d)<dp(ed))}
	opg = {}
	if 'oppt' in params:
		opid = params['oppt']
		for d in teamg:
			if str(teamg[d]['away_id']) ==  str(opid) or str(teamg[d]['home_id']) == str(opid):
				opg[team][d] = teamg[d]
				opg[tid]
		

		
	
		
		
			
	


def get_stats(team_list, season_list, td, date_range_dict=False):
	if len(team_list) < 1:
		print('No teams in team_list')
		return False
	if len(season_list) < 1:
		print('No seasons in season_list')
		return False
	if date_range_dict != False:
		drd = date_range_dict
		if 'start' not in drd:
			print('Missing start date')
			return False
		if 'end' not in drd:
			today_str = dt.today().strftime('%Y-%m-%d')
			drd['end'] = today_str

	tl = team_list
	hoa = ''
	season_boxscores = [{t:{'boxscores':{}}} for t in [*td.keys()]]
	ha = ('home', 'away')
	# tc = 0
	teamids = {td[t]['team_id']:t for t in td}
	for team in tl:
		# if tc > 1:
		# 	break
		
# 		get team id and schedules
		team_id = td[team]['team_id']
		schedule = td[team]['schedule']
		print(team_id)
		tm = str(team)
# 		put game dates in list for comprehensions
		game_dates = [*schedule.keys()]
# 		loop through schedule dates
		today = dt.today()
		boxscores = []
		td[team]['boxscores'] = boxscores
		for games in game_dates:
			# td[team]['boxscores'].append({games:{'pitching':{}, 'batting':{}}})
# 			set up dictionary for stats
			curd = dt.fromisoformat(str(games))
			
			gd = td[team]['schedule']
			print('getting stats for {0}\'s game on {1}'.format(team, games))
			if 'stats' in gd[games]:	
				if 'batting' in gd[games]['stats'] and 'pitching' in gd[games]['stats']:
					if len(gd[games]['stats']['batting']) > 0 and len(gd[games]['stats']['pitching']) > 0:
						print('skipped ')
						continue
					
			td[team]['schedule'][games]['stats'] = {'batting':{}, 'pitching':{}}
			
			# td_statsd = td[team]['schedule'][games]['stats'].copy()
			game_id = schedule[games]['game_id']
			
			# **********          sometimes the mlb api returns 503          **********
			try:
				cg = sa.get('game_boxscore', {'gamePk':game_id})
			except Exception as e:
				
				# **********          wait a second and try again          **********
				print(repr(e))
				sleep(1)
				
				try:
					cg = sa.get('game_boxscore', {'gamePk':game_id})
				except Exception:
					
					continue
				
				
				
			# **********          figure out and assign home or away	
			# **********          make vars for f
			
			
			away_id = cg['teams']['away']['team']['id']
			print(teamids.keys())
			away_team = teamids[str(away_id)]
			home_id = cg['teams']['home']['team']['id']
			print('teamid' + str(team_id) , 'awayid' + str(away_id), 'homeid'+str(home_id) )
			if str(home_id) == str(team_id):
				hoa = 'home'
				opp = 'away'
			elif str(away_id) == (team_id):
				hoa = 'away'
				opp = 'home'
			else:
				hoa = 'home'
				opp = 'away'
				print('couldnt match home or away')
			print(str('''{0} was the {1} team for their game on {2} ''').format(team, hoa, games))
			
			
			pids = [*cg['teams'][hoa]['players'].keys()]
			oids = [*cg['teams'][opp]['players'].keys()]
			players = cg['teams'][hoa]['players']
			oplayers = cg['teams'][opp]['players']
			
			bcols = ['gamesPlayed', 'flyOuts', 'groundOuts', 'runs', 'doubles', 'triples','homeRuns', 'strikeOuts', 'baseOnBalls', 'intentionalWalks', 'hits','hitByPitch', 'atBats', 'caughtStealing', 'stolenBases', 'stolenBasePercentage','groundIntoDoublePlay', 'groundIntoTriplePlay', 'plateAppearances', 'totalBases','rbi', 'leftOnBase', 'sacBunts', 'sacFlies', 'catchersInterference', 'pickoffs','atBatsPerHomeRun']
			pcols = ['flyOuts', 'groundOuts', 'airOuts', 'runs', 'doubles', 'triples', 'homeRuns', 'strikeOuts','baseOnBalls', 'intentionalWalks', 'hits', 'hitByPitch', 'atBats', 'caughtStealing', 'stolenBases', 'stolenBasePercentage','numberOfPitches', 'inningsPitched', 'holds', 'blownSaves', 'earnedRuns', 'battersFaced','outs', 'gamesPitched', 'completeGames', 'shutouts', 'pitchesThrown', 'balls', 'strikes','strikePercentage', 'hitBatsmen', 'balks','wildPitches', 'pickoffs', 'rbi', 'gamesFinished','runsScoredPer9', 'homeRunsPer9','inheritedRunners', 'inheritedRunnersScored','catchersInterference', 'sacBunts', 'sacFlies','passedBall', 'wins', 'losses', 'saves', 'saveOpportunities']
			# bcols = ['runs', 'homeRuns', 'strikeOuts', 'baseOnBalls', 'hits', 'atBats', 'stolenBases', 'plateAppearances', 'totalBases','rbi']
			# pcols = ['inningsPitched', 'runs', 'doubles', 'triples', 'homeRuns', 'strikeOuts', 'groundOuts', 'airOuts', 'pitchesThrown', 'balls', 'strikes', 'strikePercentage', 'baseOnBalls', 'intentionalWalks', 'hits', 'hitByPitch', 'atBats', 'caughtStealing', 'stolenBases',  'numberOfPitches', 'holds', 'blownSaves', 'earnedRuns', 'battersFaced', 'outs',  'completeGames', 'shutouts', 'hitBatsmen', 'balks', 'wildPitches', 'pickoffs', 'gamesFinished', 'runsScoredPer9', 'homeRunsPer9', 'inheritedRunners', 'inheritedRunnersScored', 'catchersInterference', 'sacBunts', 'sacFlies', 'passedBall', 'wins', 'losses', 'saves', 'saveOpportunities']
			curdbstat = {}
			curdpstat = {}
			
			oppdbstat = {}
			oppdpstat = {}
			
			#if date is earlier than todays date, get box score data for game
			
			if curd < today:
				 
			#### Batting	 
				curdbstat = {players[i]['person']['id']:{c:players[i]['stats']['batting'][c] for c in bcols if c in players[i]['stats']['batting']} for i in players if len(players[i]['stats']['batting']) > 1}
				oppdbstat = {oplayers[i]['person']['id']:{c:oplayers[i]['stats']['batting'][c] for c in bcols if c in oplayers[i]['stats']['batting']} for i in oplayers if len(oplayers[i]['stats']['batting']) > 1}
				for p in curdbstat:
					curdbstat[p]['name'] = players['ID' + str(p)]['person']['fullName']
				for p in oppdbstat:
					oppdbstat[p]['name'] = oplayers['ID' + str(p)]['person']['fullName']
					
					# curdbstat[p]['id'] = str(p)
				
				td[team]['schedule'][games]['stats']['batting'] = curdbstat
				if 'stats' not in td[away_team]['schedule'][games]:
					td[away_team]['schedule'][games]['stats'] = {}
					td[away_team]['schedule'][games]['stats'] = {'batting':{}, 'pitching':{}}
				td[away_team]['schedule'][games]['stats']['batting'] = oppdbstat
				
			if curd< today:
			#### Pitching	
				curdpstat = {players[i]['person']['id']:{c:players[i]['stats']['pitching'][c] for c in pcols if c in players[i]['stats']['pitching']} for i in players if len(players[i]['stats']['pitching']) > 1}
				oppdpstat = {oplayers[i]['person']['id']:{c:oplayers[i]['stats']['pitching'][c] for c in pcols if c in oplayers[i]['stats']['pitching']} for i in oplayers if len(oplayers[i]['stats']['pitching']) > 1}
				for p in curdpstat:
					curdpstat[p]['name'] = players['ID' + str(p)]['person']['fullName']
					# curdbstat[p]['id'] = str(p)
				for p in oppdpstat:
					oppdpstat[p]['name'] = oplayers['ID' + str(p)]['person']['fullName']
				
				
				td[team]['schedule'][games]['stats']['pitching'] = curdpstat
				td[away_team]['schedule'][games]['stats']['pitching'] = oppdpstat
				
			if curd < today:
				
				#### Put in a list for custom date or game range splits 
				
				
				td[team]['boxscores'].append({games:{'pitching':curdpstat, 'batting':curdbstat}})

	
	team_dict = td.copy()
# ['flyOuts', 'groundOuts', 'airOuts', 'runs', 'doubles', 'triples', 'homeRuns', 'strikeOuts',
# 'baseOnBalls', 'intentionalWalks', 'hits', 'hitByPitch', 'atBats', 'caughtStealing', 'stolenBases', 'stolenBasePercentage',
# 'numberOfPitches', 'inningsPitched', 'holds', 'blownSaves', 'earnedRuns', 'battersFaced',
# 'outs', 'gamesPitched', 'completeGames', 'shutouts', 'pitchesThrown', 'balls', 'strikes',
# 'strikePercentage', 'hitBatsmen', 'balks','wildPitches', 'pickoffs', 'rbi', 'gamesFinished',
# 'runsScoredPer9', 'homeRunsPer9','inheritedRunners', 'inheritedRunnersScored',
# 'catchersInterference', 'sacBunts', 'sacFlies','passedBall', 'wins', 'losses', 'saves', 'saveOpportunities']

				
# ['gamesPlayed', 'flyOuts', 'groundOuts', 'runs', 'doubles', 'triples',
# 'homeRuns', 'strikeOuts', 'baseOnBalls', 'intentionalWalks', 'hits',
# 'hitByPitch', 'atBats', 'caughtStealing', 'stolenBases', 'stolenBasePercentage',
# 'groundIntoDoublePlay', 'groundIntoTriplePlay', 'plateAppearances', 'totalBases',
# 'rbi', 'leftOnBase', 'sacBunts', 'sacFlies', 'catchersInterference', 'pickoffs',
# 'atBatsPerHomeRun']
# 
# 	# ['runs', 'homeRuns', 'strikeOuts', 'baseOnBalls', 'hits',
# 	# 'atBats', 'stolenBases', 'plateAppearances', 'totalBases',
	# 'rbi']	
	
	# 
	# personIds = str(statsapi.lookup_player('verlander')[0]['id']) + ',' + str(statsapi.lookup_player('rizzo')[0]['id'])
	# params = {'personIds':personIds, 'hydrate':'stats(group=[hitting,pitching],type=[statSplits],sitCodes=[vr,vl])'}
	# people = statsapi.get('people',params)
	# for person in people['people']:
	#         print('{}'.format(person['fullName']))
	#         for stat in person['stats']:
	
def statcast_matchup(pitcher, pteam, bteam):
	binary = r'C:\Program Files\Mozilla Firefox\firefox.exe'
	options = Options()
	options.headless = True
	options.binary = binary
	cap = DesiredCapabilities().FIREFOX
	cap["marionette"] = True #optional
	surl = '''https://baseballsavant.mlb.com/player_matchup?type=batter&teamPitching={0}&teamBatting={1}&player_id={2}'''
	browser = webdriver.Firefox(options=options, capabilities=cap, executable_path="C:\\Drivers\\geckodriver.exe")
	murl = surl.format(pteam, bteam, pitcher)
	print(murl)
	matchup = browser.get(surl.format(pteam, bteam, pitcher))
	tos = 10
	wdw(matchup, tos).until(ec.presence_of_element_located((By.ID, 'player_table')))
	html = matchup.page_source
	
	
	
	
	
	source = selenium_init.return_source({'surl':surl, 'pteam':pteam, 'bteam':bteam, 'pitcher':pitcher})
	soup = bs(source, features='html.parser')
	ptable = soup.find('div', id='player_table')
	print(ptable)
		
	tg = [{i['game_id']: {'home_team':i['home_name'], \
	       'home_team_id':i['home_id'], \
	       'home_prob_starter': i['home_probable_pitcher'], \
	       'away_team':i['away_name'], \
	       'away_team_id':i['away_id'], \
	       'away_prob_starter': i['away_probable_pitcher']}} \
	      for i in today_games]
#code
	
	
	# matchup = requests.get(surl.format(pteam, bteam, pitcher)).json()
	


# if len(stat['splits']):
# 				print('  {}'.format(stat['group']['displayName']))
# 			for split in stat['splits']:
# 	print('    {} {}:'.format(split['season'], split['split']['description']))
# 	for split_stat,split_stat_value in split['stat'].items():
# 	    print('      {}: {}'.format(split_stat, split_stat_value))
# 	print('\n')
# 
# 	