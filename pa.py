import os
import statsapi as sa
from datetime import datetime as dt
from datetime import timedelta as tdlt
from time import sleep

from collections import Counter as cntr
from dateutil.parser import parse as dp

# from urllib2 import urlopen, HTTPError
from bs4 import BeautifulSoup as bs
import requests as r
import json

class teams:
	
	def __init__(self):
		self.teams = {}
	
	def add_team(self, team_obj):
		self.team_to_add = team_obj
		if hasattr(self.team_to_add, 'team_name') and hasattr(self.team_to_add, 'team_id') and hasattr(self.team_to_add, 'team_abbrv'):
			team_name = self.team_to_add.team_name
			team_id = self.team_to_add.team_id
			team_abbrv = self.team_to_add.team_abbrv
			
			if team_abbrv not in self.teams:
				self.teams[team_abbrv] = {}
				self.teams[team_abbrv]['team_id'] = team_id
				self.teams[team_abbrv]['team_name'] = team_name
		tta = self.teams[team_abbrv]
		if hasattr(self.team_to_add, 'schedule'):
			
			schedule = self.team_to_add.schedule
			
			if 'schedule' not in tta:
				tta['schedule'] = {}
				tta['schedule'] = schedule
		if hasattr(self.team_to_add, 'roster'):
			roster = self.team_to_add.roster
			if 'roster' not in tta:
				tta['roster'] = {}
				tta['roster'] = roster
				

	
	#code

				
class team:
	
	def __init__(self, team_name, team_id, team_abbrv, roster={}, schedule={}):
		self.cur_team = {}
		self.team_name = team_name
		self.team_id = team_id
		self.team_abbrv = team_abbrv
		self.roster = roster
		self.schedule = schedule
		
	def make_team(self,  **kwargs):
		team = self.team_name
		abbrv = self.team_abbrv
		team_id = self.team_id
		ct = self.cur_team
		if '''roster''' in kwargs:
			roster = kwargs['roster']
		else:
			roster = {}
			#get roster here
		if abbrv not in ct:
			ct[abbrv] = {}
		if '''schedule''' in kwargs:
			schedule = kwargs['schedule']
		else:
			schedule = {}
			#get schedule here
			
		ct[abbrv] = {'team_name':team, 'team_id':team_id, 'team_abbreviation':abbrv, 'roster':roster, 'schedule':schedule}
	
	def add_roster(self, roster={}):
		if self.cur_team['roster'] == {}:
			if roster != {}:
				if 'pitchers' not in roster and 'fielders' not in roster:
					print('invalid dictionary parameter passed')
					return False
				else:
					
					if 'pitchers' in roster:
						self.cur_team['roster']['pitchers'] = {}
						for p in roster['pitchers']:
							self.cur_team['roster']['pitchers'][p] = roster['pitchers'][p]
					if 'fielders' in roster:
						self.cur_team['roster']['fielders'] = {}
						for p in roster['fielders']:
							self.cur_team['roster']['fielders'][p] = roster['fielders'][p]
	def add_schedule(self, schedule={}):
		
		if self.cur_team['schedule'] == {}:
			if schedule != {}:
				for d in schedule:
					self.cur_team['schedule'] = schedule[d]
					
				
	def add_stats(self, stats={} ):
		if self.cur_team['roster']['stats']['pitchers'] == {} and self.cur_team['roster']['stats']['fielders'] == {}:
			if stats != {}:
				dostuff = foobar
	
class get_team_info:
	
	def __init__(self):
		
		self.teams_dict = {}
		self.team_dict = {}
		self.schedule_dict = {}
		self.roster_dict = {}
		self.season_dict = {}
		self.stats_dict = {}
		
	def get_teams(self, seasons=['2022', '2021', '2020'], **kwargs):
		ms_teams_dict = {}
		mtd = ms_teams_dict
		
		
		for y in seasons:
		
			turl = """http://lookup-service-prod.mlb.com/json/named.team_all_season.bam?sport_code='mlb'&all_star_sw='N'&sort_order=name_asc&season='{0}'"""
			teams = r.get(turl.format(str(y))).json()
			cteams = teams['team_all_season']['queryResults']['row']
			
			teamdict = {t['name_abbrev']:{'team_abbrv':t['name_abbrev'], 'team_id':t['team_id'], 'team_name':t['name_display_long'], 'active_roster':{}, 'schedule':{}} for t in cteams}
			teamlist = [*teamdict.keys()]
			for t in teamlist:
				if t not in mtd:
					mtd[t] = {}
				if y not in mtd[t]:
					mtd[t][y] = {}
			
				mtd[t][y] = teamdict[t]
		return mtd


		
	def sar(self, **kwargs):
		if '''team''' not in kwargs or '''season''' not in kwargs:
			print('missing parameter(s)')
			return False
		params = {'teamId':kwargs['team'], 'season':kwargs['season'], 'rosterType':'depthChart'}
		roster = sa.get('team_roster', params)
		try:
			params = {'teamId':kwargs['team'], 'season':kwargs['season'], 'rosterType':'depthChart'}
			roster = sa.get('team_roster', params)
			r1 = roster['roster']
			return rl
		except Exception:
			params = {'teamId':kwargs['team'], 'season':kwargs['season']}
			roster = sa.get('team_roster', params)
			
			try:
				r1 = roster['roster']
				return rl
			except:
				return False
			
	def get_roster(self, team, season=['2022']):
		tid = team['team_id']
		team_roster = {}
		tr = team_roster
		for s in season:
			# roster = sa.get('team_roster', {'teamId': tid, 'rosterType':'depthChart', 'season':s})
			# print('getting roster for {0} with team id {1}'.format(str([*team.keys()][0]), str(tid)))
			
			rl = self.sar(team=tid, season=s)
			if not rl:
				continue
		
			if 'roster' not in tr:
				tr['roster'] = {}
				tr['roster'][s] = {}
				tr['roster'][s]['pitchers'] = {}
				tr['roster'][s]['fielders'] = {}
				tr['roster'][s]['fielders'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position_long':i['position']['name'],  'position_short':i['position']['abbreviation'], 'stats':{}} for i in rl if 'itcher' in i['position']['type']}
				tr['roster'][s]['pitchers'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position_long':i['position']['name'],  'position_short':i['position']['abbreviation'], 'stats':{}} for i in rl if 'itcher' not in i['position']['type']}	
			if s not in tr['roster']:
				tr['roster'][s] = {}
				tr['roster'][s]['pitchers'] = {}
				tr['roster'][s]['fielders'] = {}
				tr['roster'][s]['fielders'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position_long':i['position']['name'],  'position_short':i['position']['abbreviation'], 'stats':{}} for i in rl if 'itcher' not in i['position']['type']}
				tr['roster'][s]['pitchers'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position_long':i['position']['name'],  'position_short':i['position']['abbreviation'], 'stats':{}} for i in rl if 'itcher' in i['position']['type']}	
			
			if 'pitchers' not in tr['roster'][s]:
				tr['roster'][s]['pitchers'] = {}
				tr['roster'][s]['pitchers'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position_long':i['position']['name'],  'position_short':i['position']['abbreviation'], 'stats':{}} for i in rl if 'itcher' in i['position']['type']}
				
			if 'fielders' not in tr['roster'][s]:
				tr['roster'][s]['fielders'] = {}
				tr['roster'][s]['fielders'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position_long':i['position']['name'],  'position_short':i['position']['abbreviation'], 'stats':{}} for i in rl if 'itcher' not in i['position']['type']}
	
			if len([i for i in rl if 'itchers' in i['position']['name']]) > len(tr['roster'][s]['pitchers']):
				tr['roster'][s]['pitchers'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position_long':i['position']['name'],  'position_short':i['position']['abbreviation'], 'stats':{}} for i in rl if 'itcher' in i['position']['type']}
			if len([i for i in rl if 'itchers' in i['position']['name']]) > len(tr['roster'][s]['fielders']):
				tr['roster'][s]['fielders'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position_long':i['position']['name'],  'position_short':i['position']['abbreviation'], 'stats':{}} for i in rl if 'itcher' not in i['position']['type']}
			
		return tr
	#code	
	def construct_td(self, ):
		pass
	
				
	
class savant_data:
	
	def __init__(self, **kwargs):
		
		if 'season' in kwargs:
			self.season = kwargs['season']
		else:
			self.season = '2022'
		self.url = '''https://baseballsavant.mlb.com/player-services/statcast-pitches-breakdown?playerId={0}&position=1&pitchBreakdown=pitches&season={1}'''
	
	def get_pitch_arsenal(self, player_id, season='2022'):
		self.pid = player_id
		self.season = season
		purl = f'''https://baseballsavant.mlb.com/player-services/statcast-pitches-breakdown?playerId={self.pid}&position=1&pitchBreakdown=pitches&season={self.season}'''
		
		src = r.get(purl)
		
		if src.status_code == 200:
			soup = bs(src.content)
		else:
			print('could not retrieve data')
			return False
		ptbl = soup.find('table', id='detailedPitches')
		pp = ptbl.select('td[class*="column-two"]')
		pitches = [i.findChild('span').text for i in pp]
		if len(pitches) == 0: return False
		
		prct = [i.nextSibling.nextSibling.nextSibling.nextSibling.nextSibling.nextSibling.nextSibling.nextSibling.findChild('span').text for i in pp]
		
		return dict(zip(pitches,prct))

class game_sim:
	
	def __init__(self, team_dict, home_team={'NYY':{'id':147,'batting':[], 'pitching':[]}}, away_team={'BOS':{'id':111,'batting':[], 'pitching':[]}}, sims=1000):
		self.td = team_dict
		self.ht = home_team
		self.at = away_team
		self.sims = sims
		
		hta = [*self.ht.keys()][0]
		ata = [*self.at.keys()][0]
		if len(self.ht[hta]['batting']) == 0:
			ht_last_game = [*self.td[hta]['schedule'].keys()][-1]
			ht_last_roster = [*self.td[hta]['schedule'][ht_last_game]['stats']['batting'].keys()]
			at_last_game = [*self.td[ata]['schedule'].keys()][-1]
			at_last_roster = [*self.td[ata]['schedule'][ht_last_game]['stats']['batting'].keys()]
			
		if len(self.ht[hta]['pitching']) == 0:
			ht_last_game = [*self.td[hta]['schedule'].keys()][-1]
			ht_last_roster = [*self.td[hta]['schedule'][ht_last_game]['stats']['pitching'].keys()]
			at_last_game = [*self.td[ata]['schedule'].keys()][-1]
			at_last_roster = [*self.td[ata]['schedule'][ht_last_game]['stats']['pitching'].keys()]
			
		
	
	#code

			