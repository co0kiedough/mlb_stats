import statsapi as sa
import requests as r
import re, os, json
from bs4 import BeautifulSoup as bs
from datetime import datetime as dt
from datetime import timedelta as tdl
from time import perf_counter
from threading import Thread
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor as tpe
import logging
from dateutil.parser import parse as dp

class log:
	
	def __init__(self, ):
		pass
	
	#code


class mlb_data:

	def __init__(self):
		
		self.teams = {}
		self.team = {}
		self.schedule = {}
		self.roster = {}
		
	def log(self):
		t = dt.today().isoformat()
		t = t.split('''.''')[0]
		t = t.replace('''-''', '''_''')
		t = t.replace(''':''', '''_''')
		
		self.logger = logging.getLogger('mlb_stuff')
		self.logger.setLevel(logging.INFO)
		filename=str(t) + '''_mlb_log.txt'''
		fh = logging.FileHandler(filename)
		formatter = logging.Formatter('%(asctime)s  : %(levelname)s : %(name)s : %(message)s')
		fh.setFormatter(formatter)
		self.logger.addHandler(fh)
		
		self.logger.info(' __init__ called')
		
	def get_teams(self, **kwargs):
		self.logger.info(' get_teams function called')
		if '''season''' not in kwargs:
			season = '2022'
		else:
			season = kwargs['season']
		turl = """http://lookup-service-prod.mlb.com/json/named.team_all_season.bam?sport_code='mlb'&all_star_sw='N'&sort_order=name_asc&season='{0}'"""
		teams_resp = r.get(turl.format(season)).json()
		cteams = teams_resp['team_all_season']['queryResults']['row']
		self.teams = {t['name_abbrev']:{'team_id':t['team_id'], 'team_name':t['name_display_long'], 'schedule':{'2022':{}}, 'roster':{'2022':{}} } for t in cteams}
		
		return self.teams
	
	def get_roster(self, team, season='2022'):
		self.logger.info(''' get_roster function called for %s's %s season''', team['team_id'], season)
		team_id = team['team_id']
		team_abbrv = team['team_abbrv']
		team_roster = {}
		tr = team_roster
		tr['team_abbrv'] = team_abbrv
		tr['pitchers'] = {}
		tr['fielders'] = {}
		
		try:
			
			roster = sa.get('team_roster', {'teamId': team_id, 'rosterType':'depthChart', 'season':season})
			rl = roster['roster']
			
		except Exception as e:
			self.logger.error(''' get_roster function exception %s occured, trying again''', str(e))
			
			try:
				roster = sa.get('team_roster', {'teamId': team_id})
				rl = roster['roster']
				
			except Exception as ee:
				self.logger.error(''' get_roster function exception %s occured, second occurance, failing out''', str(ee))
				
				print('Cannot retrieve roster')
				return False
		
		tr['pitchers'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position_long':i['position']['name'],  'position_short':i['position']['abbreviation'], 'stats':{}} for i in rl if 'itcher' in i['position']['type']}
		tr['fielders'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position_long':i['position']['name'],  'position_short':i['position']['abbreviation'], 'stats':{}} for i in rl if 'itcher' not in i['position']['type']}

		return tr

	def get_stats(self, player):
		self.logger.info(''' get_stats function called - getting player %s's %s season stats''', str(player['player_id']), player['season'])
		pid = player['player_id']
		ta = player['team']
		season = player['season']
		# porb = player['group']
		group = player['group']
		print('PID:' + str(pid))
		params = {'personIds': pid, 'hydrate': 'stats(group=[{0}],type=[season],season={1})'.format(group,season)}
		try:
			pl_stats_req = sa.get('people', params)
			if 'people' not in pl_stats_req:
				print('Could not retrieve stats for {0) in the {1} season'.format(pid, season))
				return False
			else:
				if 'stats' in pl_stats_req['people'][0]:
					pl_stats = pl_stats_req['people'][0]['stats'][0]['splits'][0]['stat']
					pstats = {'player_id':pid, 'stats':pl_stats, 'team':ta}
					return pstats
				
				else:
					print('Could not retrieve stats for {0) in the {1} season'.format(pid, season))
					return False
					
		except Exception as e:
			print('Could not retrieve stats')
			print(params)
			print(e)
			return False
	
	def get_team_schedule(self, team, season='2022'):
		
		tid = team['team_id']
		tname = team['team_abbrv']
		
		sched = sa.schedule(start_date='{0}-04-03'.format(season), end_date='{0}-10-03'.format(season), team=tid)
		sched_dict = {s['game_date']:s for s in sched}
		
		return sched_dict
	
	def get_full_schedule(self, season='2022'):
		
		
		try:
			sched = sa.schedule(start_date='''{0}-04-01'''.format(season), end_date='''{0}-10-03'''.format(season))
			sched_dict = {s['game_id']:s for s in sched}
			
			return sched_dict
		except Exception as e:
			print('Error retrieving the {0} season schedule'.format(season))
			print(e)
	
	def game_boxscore(self, game):
		gameid = game['game_id']
		
		try:
			box = sa.get('game_boxscore', {'gamePk':game_id})
			return box
		except Exception as e:
			print('error retrieving boxscore')
			print(e)
			return False
			
			
		
	def rteams(self):
		td = self.get_teams()
		
		start_time = perf_counter()
		for t in td:
			ct = {'team_id':td[t]['team_id'], 'team_abbrv':str(t)}
			if '''roster''' not in td[t]:
				td[t]['roster'] = {'2022':{'pitchers':{}, 'fielders':{}}}
			if '''roster''' in td[t] and len(td[t]['roster']) == 0:
				td[t]['roster']['2022'] = {'pitchers':{}, 'fielders':{}}
			grost = self.get_roster(ct)
			td[t]['roster']['2022']['fielders'] = grost['fielders']
			td[t]['roster']['2022']['pitchers'] = grost['pitchers']
		end_time = perf_counter()
		print(f'it took {end_time - start_time :0.2f} second(s) to get team rosters')
		return td
			
	def tteams(self):
		td = self.get_teams()
		
		start_time = perf_counter()
		td_vals = [{'team_id':td[t]['team_id'], 'team_abbrv':str(t)}for t in td]
		with tpe() as executor:
			
			futures = []
			for teams in td_vals:
				futures.append(executor.submit(self.get_roster, team=teams))
			
			for future in concurrent.futures.as_completed(futures):
				res = future.result()
				td[res['team_abbrv']]['roster']['2022']['pitchers'] = res['pitchers']
				td[res['team_abbrv']]['roster']['2022']['fielders'] = res['fielders']
				
		end_time = perf_counter()
		print(f'it took {end_time - start_time :0.2f} second(s) to get team rosters')
		
		start_time = perf_counter()
		with tpe() as executor:
			
			for t in td:
				pfutures = []
				bfutures = []
				tid = td[t]['team_id']
				ta = str(t)
				
				for p in td[t]['roster']['2022']['pitchers']:
					pfutures.append(executor.submit(self.get_stats, player={'player_id':p, 'season':'2022', 'team':ta, 'group':'pitching'}))
				
				for future in concurrent.futures.as_completed(pfutures):
					res = future.result()
					
					try:
						td[res['team']]['roster']['2022']['pitchers'][res['player_id']]['stats'] = res['stats']
					except Exception as e:
						self.logger.error(''' %s error occured during tteams function for %s''', str(e), pfutures)
						continue
					
				for b in td[t]['roster']['2022']['fielders']:
					bfutures.append(executor.submit(self.get_stats, player={'player_id':b, 'season':'2022',  'team':ta, 'group':'batting'}))
				
				for future in concurrent.futures.as_completed(bfutures):
					res = future.result()
					
					try:
						td[res['team']]['roster']['2022']['fielders'][res['player_id']]['stats'] = res['stats']
					except Exception as e:
						self.logger.error(''' %s error occured during tteams function for %s''', str(e), bfutures)
						continue
		end_time = perf_counter()		
		print(f'it took {end_time - start_time :0.2f} second(s) to get player stats')	
			
		handlers = self.logger.handlers[:]
		for h in handlers:
			self.logger.removeHandler(h)
			h.close()
		
		return td
		
	
		