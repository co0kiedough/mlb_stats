import statsapi as sa
from datetime import datetime as dt
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
from alive_progress import *
from mlbsd import *


class bss:
	
	def __init__(self):
		config_handler.set_global(unknown='fishes', spinner='fish2', bar='fish')
		self.sd = savant_data()
		self.tdy = dt.today().date().isoformat()
		self.schedule = {}
		self.teams = self.get_teams()
		self.gamepks = {}
		self.roster = {}
		self.today = dt.today().strftime('%Y-%m-%d')
		self.wld = {}
		self.full_schd = {}
		self.tids = {self.teams[tm]['team_id']:tm for tm in self.teams}
		self.ta_to_tid = {tm:self.teams[tm]['team_id'] for tm in self.teams}
		
		with tpe() as executor:
			futures = []
			for t in self.teams:
				
				tid = self.teams[t]['team_id']
				ta = str(t)
				teamparams = {'team_id':tid, 'ta':ta}
				futures.append(executor.submit(self.get_team_schedule, teamparams=teamparams))
				
				
			for future in alive_it(concurrent.futures.as_completed(futures)):
				res = future.result()
				rta = res['ta']
				rts = res['sched']
				self.teams[rta]['schedule']['2022'] = rts
			
		with tpe() as executor:
			rfutures = []
			for t in self.teams:
				
				tid = self.teams[t]['team_id']
				ta = str(t)
				teamparams = {'team_id':tid, 'ta':ta}
				rfutures.append(executor.submit(self.get_roster, team=teamparams))
				
				
			for future in alive_it(concurrent.futures.as_completed(rfutures)):
				res = future.result()
				rta = res['ta']
				rtrp = res['pitchers']
				rtrf = res['fielders']
				self.teams[rta]['roster']['2022']['pitchers'] = rtrp
				self.teams[rta]['roster']['2022']['fielders'] = rtrf
		
		for tm in self.teams:
			
			ctpk = [*self.teams[tm]['roster']['2022']['pitchers'].keys()]
			ctbk = [*self.teams[tm]['roster']['2022']['fielders'].keys()]
			ctp = self.teams[tm]['roster']['2022']['pitchers']
			ctb = self.teams[tm]['roster']['2022']['fielders']
		
	def sd_ed(self, season):
		
		sdates = sa.get("seasons", {"sportId": 1, "season": season})
		rssd = sdates['seasons'][0]['regularSeasonStartDate']
		rsed = sdates['seasons'][0]['regularSeasonEndDate']
		return {'start':rssd, 'end':rsed}
	
	def box_stats_to_schd(self, wld):
		t = self.teams
		for tm in t:
			tid = t[tm]['team_id']
			schd = t[tm]['schedule']['2022']
			for g in schd:
				
				gd = schd[g]['game_date']
				if dp(gd) >= dp(self.today):
					continue
				gid = schd[g]['game_id']
				cha = self.ha(schd[g], tid)
				if 'box' not in schd[g]:
					schd[g]['box'] = wld[gid]['box']
			
		
	
	
	def plu(self, lu_params):
		
		foo = 'bar'
		player = lu_params['name']
		team = lu_params['tid']
		ta = lu_params['team_abbrv']
		p = sa.lookup_player(player)
		if len(p) > 0:
			return {'plu':p[0],'tid':team, 'team_abbrv':ta}
		else:
			return False
		
	def pitcher_type(self, pitcher_id):
		
		
		
		pid = pitcher_id
		cur = sd.get_pitch_arsenal(pid)
		if cur:
			foo=1
		
	def lgames(self, wld):
		t = self.teams
		lopg = {}
		for tm in t:
			if tm not in lopg:
				lopg[tm] = {}
		gk = [*wld.keys()]
		for g in gk:
			if wld[g]['w']['score'] - wld[g]['l']['score'] < 5:
				continue
			ha = wld[g]['l']['ha']
			ta = wld[g]['l']['team_abbrv']
			pids = wld[g]['box']['teams'][ha]['pitchers']
			ps = wld[g]['box']['teams'][ha]['players']
			for p in pids:
				curp = ps['ID'+str(p)]
				if curp['stats']['pitching']['gamesStarted'] == 1:
					rpn = float(curp['stats']['pitching']['runsScoredPer9'])
					if rpn >=5:
						lopg[ta][g] = {}
						lopg[ta][g]['sp'] = curp
		return lopg
	
					
			
		
		
		
	
	def get_starting_pitchers(self):
		t = self.teams
		tids = {t[tm]['team_id']:tm for tm in t}
		spd = {}
		pspd = {}
		requested_pitchers = []
		rp = requested_pitchers
		for tm in alive_it(t):
			tid = t[tm]['team_id']
			if tm not in spd:
				spd[tm] = {}
				spd[tm]['team_id'] = tid
				spd[tm]['team_abbrv'] = tm
				spd[tm]['sp'] = {}
			ctr = t[tm]['roster']['2022']['pitchers']
			for pl in ctr:
				sps = ctr[pl]['position_short']
				if sps == 'SP':
					spd[tm]['sp'][ctr[pl]['id']] = ctr[pl]

		if len(self.full_schd) > 0:
			schd = self.full_schd.copy()
		# 	wld = self.wld.copy()
		# 	
		else:
			schd, wld = self.get_full_schedule()
		tdy = dt.today().date().isoformat()
		gpk = [*schd.keys()]
		pparams = []
		futures = []
		for g in alive_it(gpk):
			game = schd[g]
			gd = game['game_date']
			if dp(gd) > dp(tdy):
				continue
			ht = str(game['home_id'])
			
			# hta = tids[ht]
			htsp = game['home_probable_pitcher']
			
			at = str(game['away_id'])
			# ata = tids[at]
			if ht == '160' or at == '160' or ht == '159' or at == '159':
				continue
			
			atsp = game['away_probable_pitcher']
			htparams = {}
			htparams['name'] = htsp
			htparams['tid'] = ht
			htparams['team_abbrv'] = self.tids[str(ht)]
			atparams = {}
			atparams['name'] = atsp
			atparams['tid'] = at
			atparams['team_abbrv'] = self.tids[str(at)]
			if at == '159' or ht == '159':
				continue
			if htsp not in rp:
				pparams.append(htparams)
			if atsp not in rp:
				pparams.append(atparams)
			if htsp in rp and atsp in rp:
				continue
			rp.append(htsp)
			rp.append(atsp)
			
		with tpe() as executor:
			futures = []
			for pl in pparams:
				futures.append(executor.submit(self.plu, lu_params=pl))

			
			for future in alive_it(concurrent.futures.as_completed(futures)):
				res = future.result()
				if res:
					plur = res['plu']
					pid = plur['id']
					pn = plur['fullName']
					tid = res['tid']
					ta = res['team_abbrv']
					
					
						# htr = t[hta]['roster']['2022']['pitchers']
					if pid not in spd[ta]['sp']:
						spd[ta]['sp'][pid] = {}
						spd[ta]['sp'][pid] = {'id':pid, 'name':pn, 'position_long':'Starting Pitcher', 'position_short':'SP', 'stats':{}}
					
			
		return spd	
	
	def get_teams(self, **kwargs):
		# self.logger.info(' get_teams function called')
		if '''season''' not in kwargs:
			season = '2022'
		else:
			season = kwargs['season']
		turl = """http://lookup-service-prod.mlb.com/json/named.team_all_season.bam?sport_code='mlb'&all_star_sw='N'&sort_order=name_asc&season='{0}'"""
		teams_resp = r.get(turl.format(season)).json()
		cteams = teams_resp['team_all_season']['queryResults']['row']
		self.teams = {t['name_abbrev']:{'team_abbrv':t['name_abbrev'], 'team_id':t['team_id'], 'team_name':t['name_display_long'], 'schedule':{'2022':{}}, 'roster':{'2022':{}} } for t in cteams}
		
		return self.teams
	
	def get_team_schedule(self, teamparams, season='2022'):
		ta = teamparams['ta']
		tid = teamparams['team_id']
		try:
			sdates = self.sd_ed(season)
			sd = sdates['start']
			ed = sdates['end']
		except Exception as e:
			print(e)
			return False
		sched = sa.schedule(start_date=sd, end_date=ed, team=tid)
		sched_dict = {s['game_date']:s for s in sched}
		
		return {'ta':ta,'sched':sched_dict}
	
	def get_full_schedule(self, season='2022'):
		t = self.teams
		tkd = {t[ta]['team_name']:{'team_id':t[ta]['team_id'], 'team_abbrv':t[ta]['team_abbrv'], 'team_name':t[ta]['team_name']} for ta in t}
		wld = {}
		try:
			sdates = self.sd_ed(season)
			sd = sdates['start']
			ed = sdates['end']
			sched = sa.schedule(start_date=sd, end_date=ed)
			sched_dict = {s['game_id']:s for s in sched}
			# return sched_dict
		
		except Exception as e:
			print('Error retrieving the {0} season schedule'.format(season))
			print(e)
			return False
		
		for g in alive_it(sched_dict):
			gd = sched_dict[g]['game_date']
			if dp(gd) >= dp(self.today):
				continue
			# if 'winning_team' not in sched_dict[g]:
			# 	continue
			# if 'losing_team' not in sched_dict[g]:
			# 	continue
			ha = {}
			if 'winning_team' not in sched_dict[g]:
				print(g)
				if sched_dict[g]['away_score'] > sched_dict[g]['home_score']:
					wt = sched_dict[g]['away_name']
					lt = sched_dict[g]['home_name']
				elif sched_dict[g]['away_score'] < sched_dict[g]['home_score']:
					lt = sched_dict[g]['away_name']
					wt = sched_dict[g]['home_name']
				else:
					print(g, 'cant determine winner')
				
				print(wt)
					
			else:
				wt = sched_dict[g]['winning_team']
				lt = sched_dict[g]['losing_team']
			if wt == 'American League All-Stars' or lt == 'American League All-Stars':
				continue
			
			ht = sched_dict[g]['home_name']
			at = sched_dict[g]['away_name']
			hs = sched_dict[g]['home_score']
			aws = sched_dict[g]['away_score']
			wta = tkd[wt]['team_abbrv']
			lta = tkd[lt]['team_abbrv']
			hta = tkd[ht]['team_abbrv']
			ata = tkd[at]['team_abbrv']
			if wt == ht:
				ha[wta]='home'
				ha[lta]='away'
				htw = 1
				wts = hs
				lts = aws
			if wt == at:
				ha[wta]='away'
				ha[lta]='home'
				wts = aws
				lts = hs
				htw = 0
			
				
			wld[g] = {'w':{'team_abbrv':wta,'score':wts,'ha':ha[wta]},'l':{'team_abbrv':lta,'score':lts,'ha':ha[lta]}, 'info':sched_dict[g]}
		self.wld = wld
		self.full_schd = sched_dict
		return self.full_schd.copy(), self.wld.copy()
		# wld = {g:{'w':}}d
	
	def mgame_boxscore(self, gamei):
		game_id = gamei['game_id']
		w_team_abbrv = gamei['w_team_abbrv']
		l_team_abbrv = gamei['l_team_abbrv']
		
		try:
			box = sa.get('game_boxscore', {'gamePk':game_id})
			
			return {'w_team_abbrv':w_team_abbrv,'l_team_abbrv':l_team_abbrv,'boxscore':box,'game_id':game_id}
		except Exception as e:
			print('error retrieving boxscore')
			print(e)
			return False	
	
	def game_boxscore(self, gamei):
		game_id = gamei['game_id']
		team_abbrv = gamei['team_abbrv']
		
		try:
			box = sa.get('game_boxscore', {'gamePk':game_id})
			
			return {'team_abbrv':team_abbrv,'boxscore':box,'game_id':game_id}
		except Exception as e:
			print('error retrieving boxscore')
			print(e)
			return False
	
	def multip_box(self,wldict):
		t = self.teams.copy()
		tkd = {t[ta]['team_name']:{'team_id':t[ta]['team_id'], 'team_abbrv':t[ta]['team_abbrv'], 'team_name':t[ta]['team_name']} for ta in t}
		wl = wldict.copy()
		for tm in t:
			if 'record' not in t[tm]:
				t[tm]['record'] = {'w':{},'l':{}}
				
				
		
		futures = []
		gamel = []
		for g in wl:
			if 'box' not in wl[g]:
				wl[g]['box'] = {}
			wt = wl[g]['w']['team_abbrv']
			lt = wl[g]['l']['team_abbrv']
			if g not in t[wt]['record']['w']:
				t[wt]['record']['w'][g] = {}
				t[wt]['record']['w'][g] = wl[g]
			if g not in t[lt]['record']['l']:
				t[lt]['record']['l'][g] = {}
				t[lt]['record']['l'][g] = wl[g]
			game = {}
			game['game_id'] = g
			game['w_team_abbrv'] = wt
			game['l_team_abbrv'] = lt
			
			gamel.append(game)
			
		with tpe() as executor:
			for games in gamel:
				futures.append(executor.submit(self.mgame_boxscore, gamei=games))
				# print(f'''added {str(g)} to gamel list''')

				
			for future in alive_it(concurrent.futures.as_completed(futures)):
				
				res = future.result()
				
				gameid = res['game_id']
				# print(f'''got async result for game {gameid}''')
				box = res['boxscore']
				wtb = res['w_team_abbrv']
				ltb = res['l_team_abbrv']
				t[wtb]['record']['w'][gameid]['box'] = {}
				t[ltb]['record']['l'][gameid]['box'] = {}
				t[wtb]['record']['w'][gameid]['box'] = box
				t[ltb]['record']['l'][gameid]['box'] = box
		
	
	def team_records(self):
		t = self.teams
		for tm in t:
			if 'r' not in t[tm]['record']:
				t[tm]['record']['r'] = {'w':len(t[tm]['record']['w']), 'l':len(t[tm]['record']['l'])}

	def pgames(self):
		
		t = self.teams
		pg = {tm:{t[tm]['record']['w'][g]['info']['game_id']:{'box':t[tm]['record']['w'][g]['box'], 'w':t[tm]['record']['w'][g]['w'], 'l':t[tm]['record']['w'][g]['l'], 'info':t[tm]['record']['w'][g]['info']} for g in t[tm]['record']['w'] if (t[tm]['record']['w'][g]['w']['score'] - t[tm]['record']['w'][g]['l']['score'] > 5)} for tm in t}
		lpitchers = []
		for t in pg:
			for g in pg[t]:
				gi = pg[t][g]
				wha = pg[t][g]['w']['ha']
				lha = pg[t][g]['l']['ha']
				ap = pg[t][g]['box']['teams'][lha]['players']
				apk = [*pg[t][g]['box']['teams'][lha]['players'].keys()]
				lpitchers.append([{'id':ap[p]['person']['id'], 'name':ap[p]['person']['fullName'],'stats':ap[p]['stats']} for p in apk if ap[p]['position']['abbreviation'] =='P' and len(ap[p]['stats']['pitching']) >0])
		
		return lpitchers
		# lgpd = {}
		# lp = lpitchers.copy()
		# pid = {}
		# 
		# for pl in lp:
		# 	for pli in pl:
		# 		print(pli.keys())
		# 		cg = pli['game']
		# 		if cg not in lpgd:
		# 			lpgd[cg] = {}
		# 		if pli['id'] not in lpgd[cg]:
		# 			lpgd[cg][pli['id']] = {}
		# 			lpgd[cg][pli['id']] = {'name':pli['name'], 'stats':pli['stats']['pitching']}
		# 
		# # 
		# 
		# for g in lpgd:
		# 	for pp in lpgd[g]:
		# 			n = lpgd[g][pp]['name']
		# 			if n not in pid:
		# 					pid[n] = {'count':1, 'id':pp, 'games':[g]}
		# 			else:
		# 					pid[n]['count']+=1
		# 					pid[n]['games'].append(g)
		# return lp, lpgd, pid
		# 
	def team_record(self,rtn=False):
		t = self.teams
		wld = {}
		tkd = {t[ta]['team_name']:{'team_id':t[ta]['team_id'], 'team_abbrv':t[ta]['team_abbrv'], 'team_name':t[ta]['team_name']} for ta in t}
		param_dict = {}
		boxscore_dict = {}
		start_time = perf_counter()
		for tm in t:
			team_time = perf_counter()
			tn = t[tm]['team_name']
			tid = t[tm]['team_id']
			if '''record''' not in t[tm]:
				t[tm]['record'] = {'w':{},'l':{}}
				
			s = t[tm]['schedule']['2022']
			sd = [*s.keys()]
			
			for d in sd:
				if dp(d) < dp(self.today):
					g = s[d]
					gid = g['game_id']
					# if gid not in boxscore_dict:
					# 	boxscore_dict[gid] = {}
					if gid not in wld:
						wld[gid] = {}
						
					if '''winning_team''' not in g:
						continue
					else:
						
						wt = g['winning_team']
						lt = g['losing_team']
						wld[gid] = {'w':tkd[wt]['team_abbrv'], 'l':tkd[lt]['team_abbrv']} 
						game = {}
						game['game_id'] = gid
						# resp = self.game_boxscore(game)
						
						try:
							if wt == tn:
								t[tm]['record']['w'][gid] = g
								t[tm]['record']['w'][gid]['boxscore'] = {}
							
							else:
								t[tm]['record']['l'][gid] = g
								t[tm]['record']['l'][gid]['boxscore'] = {}
							
							
						except Exception as e:
							print(e)
							continue
			
			recw = t[tm]['record']['w']
			tdw_vals = [{'game_id':recw[g]['game_id'], 'team_abbrv':t} for g in recw]
			recl = t[tm]['record']['l']
			tdl_vals = [{'game_id':recl[g]['game_id'], 'team_abbrv':t} for g in recl]
			
			with tpe() as executor:
				
				wfutures = []
				lfutures = []
				for game in tdw_vals:
					wfutures.append(executor.submit(self.game_boxscore, gamei=game))
				for game in tdl_vals:
					lfutures.append(executor.submit(self.game_boxscore, gamei=game))
					
				for future in concurrent.futures.as_completed(wfutures):
					res = future.result()
					gameid = res['game_id']
					box = res['boxscore']
					if gameid in boxscore_dict and len(boxscore_dict[gameid]) < 1:
						boxscore_dict[gameid] = box
					tab = res['team_abbrv']
					t[tm]['record']['w'][gameid]['boxscore'] = box
				for future in concurrent.futures.as_completed(lfutures):
					res = future.result()
					gameid = res['game_id']
					box = res['boxscore']
					if gameid in boxscore_dict and len(boxscore_dict[gameid]) < 1:
						boxscore_dict[gameid] = box
					tab = res['team_abbrv']
					t[tm]['record']['l'][gameid]['boxscore'] = box
			eteam_time = perf_counter()
			print(f'it took {eteam_time - team_time :0.2f} second(s) to get the {tm} roster')
		end_time = perf_counter()
		print(f'it took {end_time - start_time :0.2f} second(s) to get team rosters')
		if rtn != False:
			return boxscore_dict
	
	def box_stat_handler(self, params, **kwargs):
		batting_cols = ['gamesPlayed', 'flyOuts', 'groundOuts', 'runs', 'doubles', 'triples', 'homeRuns', 'strikeOuts', 'baseOnBalls', 'intentionalWalks', 'hits', 'hitByPitch', 'atBats', 'caughtStealing', 'stolenBases', 'stolenBasePercentage', 'groundIntoDoublePlay', 'groundIntoTriplePlay', 'plateAppearances', 'totalBases', 'rbi', 'leftOnBase', 'sacBunts', 'sacFlies', 'catchersInterference', 'pickoffs', 'atBatsPerHomeRun']
		pitching_cols = ['note', 'gamesPlayed', 'gamesStarted', 'flyOuts', 'groundOuts', 'airOuts', 'runs', 'doubles', 'triples', 'homeRuns', 'strikeOuts', 'baseOnBalls', 'intentionalWalks', 'hits', 'hitByPitch', 'atBats', 'caughtStealing', 'stolenBases', 'stolenBasePercentage', 'numberOfPitches', 'inningsPitched', 'wins', 'losses', 'saves', 'saveOpportunities', 'holds', 'blownSaves', 'earnedRuns', 'battersFaced', 'outs', 'gamesPitched', 'completeGames', 'shutouts', 'pitchesThrown', 'balls', 'strikes', 'strikePercentage', 'hitBatsmen', 'balks', 'wildPitches', 'pickoffs', 'rbi', 'gamesFinished', 'runsScoredPer9', 'homeRunsPer9', 'inheritedRunners', 'inheritedRunnersScored', 'catchersInterference', 'sacBunts', 'sacFlies', 'passedBall']
		
		
	def ha(self, game, team):
		at = game['away_id']
		ht = game['home_id']
		if team ==at:
			return 'a'
		elif team == ht:
			return 'h'
		else:
			return False
			
	def stats_by_range(self, pid_list, b_or_p, season='2022', sd=False, ed=False):
		if b_or_p != '''batting''' and b_or_p !='''pitching''':
			return False
		else:
			group = b_or_p
		
		if len(pid_list) > 1:
			pidss = [str(p) for p in pid_list]
			pids = ','.join(pidss)
		elif len(pid_list) == 1:
			pids = str(pid_list[0])
		else:
			return False
		
		sded = self.sd_ed(season)
		if sded != False:
			if sd == False:
				asd =sded['start']
			else:
				if dp(sd) >= dp(sded['start']):
					asd = sd
				else:
					return False
				
			if ed == False:
				aed = sded['end']
			else:
				if dp(ed) <= dp(sded['end']):
					aed = ed
				else:
					return False
		else:
			return False
		params = {"personIds": pids, "hydrate": f"stats(group=[{group}],type=[byDateRange],startDate={asd},endDate={aed},season={season})"}
		ps = sa.get('people', params)
		sc = [*ps['people'][0]['stats'][0]['splits'][0]['stat'].keys()]
		# sv = ps['people'][p]['stats'][0]['splits'][0]['stat']
		resp_stat = {p['id']:{c:p['stats'][0]['splits'][0]['stat'][c] for c in sc} for p in ps['people']}
		return resp_stat
# 		aribat['people'][p]['stats'][0]['splits'][0]
	
	def box_stats_util(self, params_dict):
		pd = params_dict
		ghad = {}

	
	def get_roster(self, team, season='2022'):
		# self.logger.info(''' get_roster function called for %s's %s season''', team['team_id'], season)
		team_id = team['team_id']
		team_abbrv = team['ta']
		team_roster = {}
		tr = team_roster
		tr['team_abbrv'] = team_abbrv
		tr['pitchers'] = {}
		tr['fielders'] = {}
		
		try:
			
			roster = sa.get('team_roster', {'teamId': team_id, 'rosterType':'depthChart', 'season':season})
			rl = roster['roster']
			
		except Exception as e:
			# self.logger.error(''' get_roster function exception %s occured, trying again''', str(e))
			
			try:
				roster = sa.get('team_roster', {'teamId': team_id})
				rl = roster['roster']
				
			except Exception as ee:
				# self.logger.error(''' get_roster function exception %s occured, second occurance, failing out''', str(ee))
				
				print('Cannot retrieve roster')
				return False
		
		tr['pitchers'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position_long':i['position']['name'],  'position_short':i['position']['abbreviation'], 'stats':{}} for i in rl if 'itcher' in i['position']['type']}
		tr['fielders'] = {i['person']['id']:{'id':i['person']['id'], 'name':i['person']['fullName'], 'position_long':i['position']['name'],  'position_short':i['position']['abbreviation'], 'stats':{}} for i in rl if 'itcher' not in i['position']['type']}
		tr['ta'] = team_abbrv
		return tr
