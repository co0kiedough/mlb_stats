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


class savant_data:
	
	def __init__(self, **kwargs):
		
		if 'season' in kwargs:
			self.season = kwargs['season']
		else:
			self.season = '2022'
		self.ptypes = sa.meta('pitchTypes')
		self.pd = {pt['code']:pt['description'] for pt in self.ptypes}
		self.pcodes = [*self.pd.keys()]
		self.pg = {'OS':'Offspeed', 'FF':'Fastball','CB':'Breaking'}
		
		self.pid = ''
		self.url = '''https://baseballsavant.mlb.com/player-services/statcast-pitches-breakdown?playerId={0}&position=1&pitchBreakdown=pitches&season={1}'''
		
		
	
	def urls(self, pid, season='2022', **kwargs):
# 		kwargs options
# 		p_or_b - pitcher or batter - 'p' or 'b' are accepted str
# 		g_or_t - pitch group or pitch type - 'g' or 't' are accepted str
# 		pg - pitch groups for when g_or_t == g - 'FF', 'OS', 'CB' are accepted - 'Fastball', 'Offspeed', 'Curveball/Breaking' respectively
# 		pt - pitch type code for when g_or_t == t
# 		pa - put away % - sets count to 2 strikes to retrieve put away stats
		print(kwargs)
		if len(kwargs) == 0:
			print(kwargs)
			return False
		
		if 'p_or_b' not in kwargs:
			print(kwargs)
			return False
		position = ''
		breakdown = ''
		pitchType = ''
		self.pid = pid
		self.season = season
		pcount = ''
		
		if kwargs['p_or_b'] == 'p':
			position = '1'
		elif kwargs['p_or_b'] == 'b':
			position = '4'
		else:
			position = '1'
			
		
		if 'g_or_t' not in kwargs:
			g_or_t = 't'
		else:
			g_or_t = kwargs['g_or_t']
		if g_or_t == 'g':
			breakdown = '''pitch-group'''
			if '''pg''' not in kwargs:
				pitchType = ''
			else:
				if kwargs['pg'] not in self.pg:
					pitchType = ''
				else:
					pitchType = self.pg[kwargs['pg']]
		elif g_or_t == 't':
			breakdown = 'pitches'
			if '''pt''' not in kwargs:
				pitchType = ''
			else:
				kpt = kwargs['pt']
				if kpt not in self.pcodes:
					pitchType = ''
				else:
					pitchType = kpt
		else:
			breakdown = 'pitches'
			pitchType = ''
						
		if '''pa''' in kwargs:
			if kwargs['pa'] == 1:
				pcount = '2strikes'
			else:
				pcount = ''
		else:
			pcount = ''
			
		self.url_two_strikes = f'''https://baseballsavant.mlb.com/player-services/statcast-pitches-breakdown?playerId={self.pid}&position={position}&hand=&pitchBreakdown={breakdown}&pitchType={pitchType}&timeFrame=yearly&season={self.season}&count={pcount}&updatePitches=false'''
		return self.url_two_strikes
	
	
	def get_hit_stats(self,player_id, season='2022', **kwargs):
		
		#pass 'pa'=1 in kwargs for 2Strike stats
		#pass 'g_or_t'='g' and 'pg'=['','FF','OS','CB']  in kwargs & code for specific pitch group stats
		#pass 'g_or_t'='t' and 'pt'=
		#['','FA','FF','FT','FC','FS','FO','SI','ST','SL','CU','KC','SC','GY','SV','CS','CH','KN','EP','UN','IN','PO','AB','AS','NP']
		# in kwargs & code for specific pitch type stats
		params = {}
		params['p_or_b'] = 'b'
		if len(kwargs) > 0:
			if 'pa' in kwargs:
				params['pa'] = kwargs['pa']
			if 'g_or_t' in kwargs:
				if kwargs['g_or_t'] == 'g':
					params['g_or_t'] = 'g'
					if 'pg' in kwargs:
						if kwargs['pg'] in self.pg:
							
							params['pg'] = kwargs['pg']
						else:
							params['pg'] = ''
					else:
						params['pg'] = ''
				else:
					params['g_or_t'] = 't'
					if 'pt' in kwargs:
						if kwargs['pt'] in self.pd:
							params['pt'] = kwargs['pt']
						else:
							params['pt'] = ''
			
		self.pid = player_id
		self.season = season
		print(params)
		burl = self.urls(self.pid, self.season, **params)
		
		print(burl)
		
		src = r.get(burl)
		# print(burl)
		if src.status_code == 200:
			soup = bs(src.content, features='lxml')
		else:
			print('could not retrieve data')
			return False
		
		ptbl = soup.find('table', id='detailedPitches')
		thead = ptbl.find('thead')
		ths = thead.find_all('th')
		bcols = [str(ts.string).replace('''\n''','').strip() for ts in ths if len(str(ts.string).replace('''\n''','').strip()) > 0]
		# print(bcols)
		# return
		tbod = ptbl.find('tbody')
		trows = tbod.find_all('tr')
		trl = len(trows)
		season_vs_pitch = {}
		
		for tr in range(trl):
			row_vals = []
			ctr = trows[tr].find_all('td')
			for c in ctr:
				span = c.find_all('span')
				if len(span) > 0:
					row_vals.append(str(span[0].string).replace('''\n''','').strip())
				else:
					row_vals.append(str(c.string).replace('''\n''','').strip())
			if int(row_vals[2]) < 5:
				continue
			else:
				pd = dict(zip(bcols, row_vals))
				if pd['Pitch Type'] not in season_vs_pitch:
					season_vs_pitch[pd['Pitch Type']] = pd
		# return
		return season_vs_pitch
		
		
		
	def get_pitch_arsenal(self, player_id, season='2022'):
		self.pid = player_id
		self.season = season
		purl = f'''https://baseballsavant.mlb.com/player-services/statcast-pitches-breakdown?playerId={self.pid}&position=1&pitchBreakdown=pitches&season={self.season}'''
		
		src = r.get(purl)
		
		if src.status_code == 200:
			soup = bs(src.content, features="lxml")
		else:
			print('could not retrieve data')
			return False
		ptbl = soup.find('table', id='detailedPitches')
		pp = ptbl.select('td[class*="column-two"]')
		pitches = [i.findChild('span').text for i in pp]
		if len(pitches) == 0: return False
		prct = [i.nextSibling.nextSibling.nextSibling.nextSibling.nextSibling.nextSibling.nextSibling.nextSibling.findChild('span').text for i in pp]
		
		return {self.pid:dict(zip(pitches,prct))}

	
	def pitcher_type(self, ):
		pass
		