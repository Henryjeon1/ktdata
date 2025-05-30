import os
import pymysql
import pandas as pd
import numpy as np

# ✅ 환경 변수에서 날짜 가져오기
PW = os.getenv("PW")


db = pymysql.connect(host='14.49.30.59', port = 33067, user = 'ktwiz', passwd = PW, db = 'ktwiz')


cursor = db.cursor()
sql = """
SELECT

-- game id
gameid

-- pitch_type
, PITKIND AS pitch_type

-- date
, `DATE`

-- release_speed
, ROUND( Relspeed*0.621371,1) as release_speed

-- release_pos_x
, ROUND(RelSide*3.28084,2) AS release_pos_x

-- release_pos_z
, ROUND(RelHeight*3.28084,2) AS release_pos_z

-- pitname
, pitname

-- batname
, batname

-- batter
, batterid  AS batter

-- pitcher
, pitcherid as pitcher

-- events
, case when playresult = 'Out' then 'field_out'
	when playresult = 'FieldersChoice' then 'fielders_choice_out'
	when playresult = 'Error' then 'field_error'
	when playresult = 'Single' then 'single'
	when playresult = 'Double' then 'double'
	when playresult = 'Triple' then 'triple'
	when playresult = 'HomeRun' then 'home_run'
	when playresult = 'Sacrifice' and distance >= 30 then 'sac_fly'
	when (playresult = 'Sacrifice' and distance < 30) or (playresult = 'Sacrifice' and TaggedHitType = "Bunt") then 'sac_bunt'
	when playresult = 'HitByPitch' or pitchcall = 'HitByPitch'  then 'hit_by_pitch'
	when playresult = 'Undefined' and korbb = 'Strikeout' then 'strikeout'
	when playresult = 'Undefined' and korbb = 'Walk' then 'walk' else NULL END as events

-- description
, case when pitchCall in('FoulBall', 'FoulballFieldable', 'FoulballNotFieldable') then 'foul'
	when pitchCall in ('BallCalled', 'BallinDirt', 'BallAutomatic') then 'ball'
	when pitchCall in ('StrikeCalled', 'AutomaticStrike', 'StrikeAutomatic') then 'called_strike'
	when pitchCall = 'StrikeSwinging' then 'swinging_strike'
	when pitchCall = 'InPlay' and playresult in ('Double','Single','HomeRun','Error','Triple','FieldersChoice')  and  runsscored = 0 then 'hit_into_play_no_out'
	when pitchCall = 'InPlay' and runsscored >= 1 then 'hit_into_play_score'
	when pitchCall = 'InPlay' and playresult in ('Out','Sacrifice') and runsscored = 0 then 'hit_into_play'
	when pitchCall = 'HitByPitch' or playresult = 'HitByPitch'  then 'hit_by_pitch'
	when pitchCall = 'BallIntentional' then 'pitchout'
 	else null END AS description

-- zone
, '' AS 'zone'

-- des
, 'des'

-- stand
, CASE WHEN BATTERSIDE = 'RIGHT' THEN 'R' WHEN BATTERSIDE = 'LEFT' THEN 'L' ELSE NULL END AS stand

-- p_throws
, CASE WHEN PITCHERTHROWS = 'RIGHT' THEN 'R' WHEN PITCHERTHROWS = 'LEFT' THEN 'L' ELSE NULL END  AS p_throws

-- pitcherteam
, pitcherteam

-- batterteam
, batterteam

-- hometeam, awayteam
, HomeTeam, AwayTeam

-- type
, CASE WHEN pitchCall IN ('BallinDirt','BallCalled','HitByPitch','BallIntentional','BallAutomatic') THEN 'B'
	WHEN PITCHCALL IN ('StrikeCalled','FoulBall','StrikeSwinging','StrikeAutomatic','FoulBallNotFieldable','FoulBallFieldable') THEN 'S' 
	WHEN PITCHCALL IN ('InPlay') THEN 'X' ELSE NULL END AS 'type'

-- bb_type
-- , CASE WHEN pitchcall='inplay' AND exitspeed >= 1 and angle < 10 then 'Groundball'
-- 	WHEN pitchcall='inplay' AND exitspeed >= 1 and angle >= 25 and angle < 50 then 'Flyball'
-- 	WHEN pitchcall='inplay' AND exitspeed >= 1 and angle >= 10 and angle < 25 then 'LineDrive'
-- 	WHEN pitchcall='inplay' AND exitspeed >= 1 and angle >= 50 then 'Popup' ELSE NULL END AS bb_type          -- bb_type 계산용
, case when pitchcall  = 'inplay' and TaggedHitType = 'GroundBall' then 'Ground_Ball' 
	when pitchcall  = 'inplay' and TaggedHitType = 'LineDrive' then 'Line_Drive'
	when pitchcall  = 'inplay' and TaggedHitType = 'FlyBall' then 'Fly_Ball'
	when pitchcall  = 'inplay' and TaggedHitType = 'Popup' then 'Popup'
	when pitchcall  = 'inplay' and TaggedHitType = 'Bunt' then 'Bunt'
	else null end as bb_type

-- count
, balls , strikes 

-- pfx_x, pfx_z
, HorzBreak * 0.032808 AS pfx_x , InducedVertBreak *0.032808 as pfx_z

-- plate_x, plate_z
, PlateLocSide as plate_x	, PlateLocHeight as plate_z

-- outs
,  Outs AS outs_when_up

-- inning
, inning

-- topbot
, Top_Bottom as inning_topbot
 
 -- hit_distance_sc
, round(DISTANCE * 3.28084,0) AS hit_distance_sc

-- launch speed
, round(EXITSPEED * 0.621371 ,1) as launch_speed

-- launch angle
, round(ANGLE,0) AS launch_angle

-- release_extension
, round(SpinRate,0) as release_spin_rate	, round(SpinAxis,1) as release_spin_axis ,  round(Extension * 3.28084,2) as release_extension

-- launch speed angle
, case 
	when (EXITSPEED/ 1.609344 * 1.5 - angle) >= 117
	and (EXITSPEED/ 1.609344 + angle) >= 124
	and EXITSPEED/ 1.609344 >= 98
	and angle between 4 and 50
	and pitchcall = 'InPlay'
	then 6 -- 'Barrel'

	when (EXITSPEED/ 1.609344 * 1.5 - angle) >= 111
	and (EXITSPEED/ 1.609344 + angle) >= 119
	and EXITSPEED/ 1.609344 >= 95
	and angle between 0 and 52
	and pitchcall = 'InPlay'
	then 5 -- 'Solid-Contact'
   
	when EXITSPEED/ 1.609344 <= 59
	and pitchcall = 'InPlay'
	then 1 -- 'Poorly-Weak'
 
	when (EXITSPEED/ 1.609344 * 2 - angle) >= 87
	and angle <= 41
	and (EXITSPEED/ 1.609344 * 2 + angle) <= 175
	and (EXITSPEED/ 1.609344 + angle * 1.3) >= 89
	and EXITSPEED/ 1.609344 between 59 and 72
	and pitchcall = 'InPlay'
 	then 4 -- 'Flare-or-Burner'
   
	when (EXITSPEED/ 1.609344 + angle * 1.3) <= 112
	and (EXITSPEED/ 1.609344 + angle * 1.55) >= 92
	and EXITSPEED/ 1.609344 between 72 and 86
	and pitchcall = 'InPlay'
	then 4 -- 'Flare-or-Burner'
   
	when angle <= 20
	and (EXITSPEED/ 1.609344 + angle * 2.4) >= 98
	and EXITSPEED/ 1.609344 between 86 and 95
	and pitchcall = 'InPlay'
	then 4 -- 'Flare-or-Burner'
   
	when (EXITSPEED/ 1.609344 - angle) >= 76
	and (EXITSPEED/ 1.609344 + angle * 2.4) >= 98
	and EXITSPEED/ 1.609344 >= 95
	and angle <= 30
	and pitchcall = 'InPlay'
 	then 4 -- 'Flare-or-Burner'

	when (EXITSPEED/ 1.609344 + angle * 2) >= 116
	and pitchcall = 'InPlay'
	then 3 -- 'Poorly-Under'

	when (EXITSPEED/ 1.609344 + angle * 2) <= 116
	and pitchcall = 'InPlay'
	then 2 -- 'Poorly-Topped'

   else NULL -- 'Unclassified'
 end AS launch_speed_angle

 -- pitch_number
, pitchofpa AS pitch_number

-- pa of inning
, paofinning

-- pitch name
, CASE WHEN PITKIND = 'FF' THEN '4-Seam Fastball'
	WHEN PITKIND = 'CH' THEN 'Changeup'
	WHEN PITKIND = 'SL' THEN 'Slider'
	WHEN PITKIND = 'CU' THEN 'Curveball'
	WHEN PITKIND = 'ST' THEN 'Sweeper'
	WHEN PITKIND = 'FS' THEN 'Split-Finger'
	WHEN PITKIND IN ('FT','SI') THEN '2-Seam Fastball'
	WHEN PITKIND = 'FC' THEN 'Cutter' ELSE PITKIND END AS pitch_name
-- , PlateLocSide, PlateLocHeight

, home_score_cn
, away_score_cn
, LEVEL
, vertrelangle
, case when bearing >= 0 then 'R' WHEN BEARING < 0 THEN 'L' ELSE NULL END as direction
, round(ContactPositionX,2) , round(ContactPositionY,2) , round(ContactPositionZ,2)

, case when pitchcall = 'inplay' and bearing >= 0 and bearing <= 45 then convert(round(distance *cos(radians(45-bearing)),0),int)
   when pitchcall = 'inplay' and bearing < 0 and bearing >= -45 then convert(round(distance *cos(radians(45 + abs(bearing))),0),int) else '' end as groundxside
, case when pitchcall = 'inplay' and bearing >= 0 and bearing <= 45 then convert(round(distance *sin(radians(45-bearing)),0),int)
   when pitchcall = 'inplay' and bearing < 0 and bearing >= -45 then convert(round(distance *sin(radians(45 + abs(bearing))),0),int) else '' end as groundyside

-- year
, SEASON AS game_year

-- hit spin rat
, hitspinrate

-- catcher
, catcher as catcher

-- trace
, px0, x5, x10, x15, x20, x25, x30, x35, x40, x45, x50
, pz0, z5, z10, z15, z20, z25, z30, z35, z40, z45, z50

 -- pitkind
	FROM 
		
		(
		SELECT a.*, substring(GameID ,1,4) as SEASON
		, CASE WHEN pit_kind_cd = '31' THEN 'FF'
		WHEN pit_kind_cd = '32' THEN 'CU'
		WHEN pit_kind_cd = '33' THEN 'SL'
		WHEN pit_kind_cd = '34' THEN 'CH'
		WHEN pit_kind_cd = '35' THEN 'FS'
		WHEN pit_kind_cd = '36' THEN 'SI'
		WHEN pit_kind_cd = '37' THEN 'FT'
		WHEN pit_kind_cd = '38' THEN 'FC'
  		WHEN pit_kind_cd = '131' THEN 'ST'
  		WHEN PIT_KIND_CD IS NULL and (AUTOPITCHTYPE = 'Fastball' OR AUTOPITCHTYPE = 'Four-Seam')  then 'FF'
		WHEN PIT_KIND_CD IS NULL and AUTOPITCHTYPE = 'Sinker' then 'SI' 
		WHEN PIT_KIND_CD IS NULL and AUTOPITCHTYPE = 'Curveball' then 'CU' 
		WHEN PIT_KIND_CD IS NULL and AUTOPITCHTYPE = 'Slider' then 'SL'
		WHEN PIT_KIND_CD IS NULL and AUTOPITCHTYPE = 'Changeup' then 'CH'
		WHEN PIT_KIND_CD IS NULL and AUTOPITCHTYPE = 'Splitter' then 'FS'
		WHEN PIT_KIND_CD IS NULL and AUTOPITCHTYPE = 'Cutter' then 'FC'
  		WHEN PIT_KIND_CD IS NULL and AUTOPITCHTYPE = 'Sweeper' then 'ST'
		ELSE 'OT' END  AS PITKIND
		
		, PITCHER as pitname, BATTER as batname 
	 
		, POS1_P_ID, POS2_P_ID, POS3_P_ID, POS4_P_ID, POS5_P_ID, POS6_P_ID, POS7_P_ID, POS8_P_ID, POS9_P_ID
		, home_score_cn , away_score_cn
 		, c.x0 as px0, x5, x10, x15, x20, x25, x30, x35, x40, x45, x50
		, c.z0 as pz0, z5, z10, z15, z20, z25, z30, z35, z40, z45, z50

		FROM 
			pda_trackman a
		  -- JOIN
			Left outer join 
			pda_analyzer b
			ON a.game_seq = b.game_seq AND a.pit_seq = b.pit_seq
   			Left outer join 
			pda_calculate c
			ON a.game_seq = c.game_seq AND a.PitchNo = c.pitch_no
			
		) ZZ
	 
	
WHERE 

substring(gameid,1,4) >= '2021'
and gameid <> '20241001-Suwon-1'
and level = 'KBO Minors'

order by gameid, pitchno 

"""



cursor.execute(sql)
raw = cursor.fetchall()

df=pd.DataFrame(raw, columns = ['game_id','pitch_type', 'game_date', 'release_speed', 'release_pos_x', 'release_pos_z',  'pitname', 'batname', 'batter', 'pitcher', 'events', 'description', 'zone', 'des', 'stand', 'p_throw', 'pitcherteam','batterteam', 'hometeam' , 'awayteam',
                                'type', 'bb_type', 'balls', 'strikes', 'pfx_x', 'pfx_z', 'plate_x', 'plate_z', 'out_when_up', 'inning', 'inning_topbot', 'hit_distance_sc',
                                'launch_speed','launch_angle','release_spin_rate','release_spin_axis', 'release_extension',
                                'launch_speed_angle','pitch_number','PAofinning','pitch_name','home_score','away_score','level','verrelangle','launch_direction', 'contactX' , 'contactY' , 'contactZ', 'groundX','groundY','game_year','hit_spin_rate', 'catcher',
                                'x0', 'x5', 'x10', 'x15', 'x20', 'x25', 'x30', 'x35', 'x40', 'x45', 'x50',
                                'z0', 'z5', 'z10', 'z15', 'z20', 'z25', 'z30', 'z35', 'z40', 'z45', 'z50'])

df['plate_x'] = df['plate_x'] * -1.0
df['pfx_x'] = df['pfx_x'] * -1.0


cursor = db.cursor()
height_sql = """
select * from player_info  """

cursor.execute(height_sql)
height_raw = cursor.fetchall()

height_df=pd.DataFrame(height_raw, columns = ['game_year', 'team', 'team_tm_major', 'team_tm_minor','KBO_ID','TM_ID','NAME','POS','BackNum','engName','Birthday','Height','Weight','PitcherThrows','BatterSide'])

height_df['TM_ID'] = height_df['TM_ID'].astype(str)
height_df['game_year'] = height_df['game_year'].astype(str)


height_df_h = height_df.rename(columns={'NAME': 'NAME_batter'})
df = pd.merge(left = df , right = height_df_h, how = "left", left_on = ["game_year","batter"], right_on = ["game_year", "TM_ID"] )

height_df_p = height_df.rename(columns={'NAME': 'NAME_pitcher'})
height_df_p = height_df_p[["game_year", "TM_ID", "NAME_pitcher"]]
df = pd.merge(left = df , right = height_df_p, how = "left", left_on = ["game_year","pitcher"], right_on = ["game_year", "TM_ID"] )



df[['batter','pitcher','groundX','groundY','game_year']] = df[['batter','pitcher','groundX', 'groundY','game_year']].apply(pd.to_numeric)

conv_fac = 0.3048

df['rel_height'] = df['release_pos_z']*conv_fac
df['rel_side'] = df['release_pos_x']*conv_fac
df['hor_break'] = df['pfx_x']*conv_fac*100
df['ver_break'] = df['pfx_z']*conv_fac*100

p_type = df[['pitcher','rel_height']]
rel_height = p_type.groupby(['pitcher'], as_index=False).mean()
rel_height['type'] = rel_height['rel_height'].apply(lambda x: 'S' if x <= 1.5 else 'R')
side_arm = rel_height[rel_height['type'] == 'S']

y = side_arm['pitcher'].unique()

df['side_arm'] = df['pitcher'].apply(lambda x: 'S' if x in y else 'Reg')
df['p_throws'] = df['pitcher'].apply(lambda x: 'S' if x in y else (list(set(df.loc[df['pitcher'] == x, 'p_throw'].unique()))+[None])[0])

df['Height'] = pd.to_numeric(df['Height'], errors='coerce')

# 신장 가져오기
df['high'] = np.where(df['Height'].isnull(), 1.049 , (df['Height'] * 0.5575 + 3.5) / 100)
df['low']  = np.where(df['Height'].isnull(), 0.463 , (df['Height'] * 0.2704 - 3.5) / 100)
# 신장이 없는 경우는 일단 183cm 기준으로 가져오는 것으로 함 


df['1/3'] = (df['low'] + ((df['high'] - df['low']) / 3))
df['2/3'] = (df['high'] - ((df['high'] - df['low']) / 3))

# df['zonehigh'] = df['Height'] * 0.6335 / 100
# df['corehigh'] = df['Height'] * 0.4935 / 100
# df['corelow'] = df['Height'] * 0.3464 / 100
# df['zonelow'] = df['Height'] * 0.2064 / 100


df['zonehigh'] = df['high'] + 0.11
df['corehigh'] = df['high'] - 0.11
df['corelow'] = df['low'] + 0.11
df['zonelow'] = df['low'] - 0.11

condition9 = [
             (df['plate_x'] > -0.271) & (df['plate_x'] < 0.271) & (df['plate_z'] > df['low'] ) & (df['plate_z'] < df['high']),
             (df['plate_x'] > -0.381) & (df['plate_x'] < 0.381) & (df['plate_z'] > df['zonelow']) & (df['plate_z'] < df['zonehigh'])
             
]

choicelist9 = ['IN', 'OUT']

df['INOUT'] = np.select(condition9, choicelist9, default= 'Not Specified')

condition0 = [
             (df['plate_x'] > -0.271) & (df['plate_x'] < -0.0903) & (df['plate_z'] > df['2/3']) & (df['plate_z'] < df['high']),
             (df['plate_x'] > -0.0903) & (df['plate_x'] < 0.0903) & (df['plate_z'] > df['2/3']) & (df['plate_z'] < df['high']),
             (df['plate_x'] > 0.0903) & (df['plate_x'] < 0.271) & (df['plate_z'] > df['2/3']) & (df['plate_z'] < df['high']),
             
             (df['plate_x'] > -0.271) & (df['plate_x'] < -0.0903) & (df['plate_z'] > df['1/3']) & (df['plate_z'] < df['2/3']),
             (df['plate_x'] > -0.0903) & (df['plate_x'] < 0.0903) & (df['plate_z'] > df['1/3']) & (df['plate_z'] < df['2/3']),
             (df['plate_x'] > 0.0903) & (df['plate_x'] < 0.271) & (df['plate_z'] > df['1/3']) & (df['plate_z'] < df['2/3']),
             
             (df['plate_x'] > -0.271) & (df['plate_x'] < -0.0903) & (df['plate_z'] > df['low']) & (df['plate_z'] < df['1/3']),
             (df['plate_x'] > -0.0903) & (df['plate_x'] < 0.0903) & (df['plate_z'] > df['low']) & (df['plate_z'] < df['1/3']),
             (df['plate_x'] > 0.0903) & (df['plate_x'] < 0.271) & (df['plate_z'] > df['low']) & (df['plate_z'] < df['1/3']),
             
             (df['INOUT'] == 'OUT') & (df['plate_x'] < 0) & (df['plate_z'] > 0.75) ,
             (df['INOUT'] == 'OUT') & (df['plate_x'] > 0) & (df['plate_z'] > 0.75) ,
             (df['INOUT'] == 'OUT') & (df['plate_x'] < 0) & (df['plate_z'] < 0.75) ,
             (df['INOUT'] == 'OUT') & (df['plate_x'] > 0) & (df['plate_z'] < 0.75) ,
             
             
]

choicelist0 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14]

df['zone'] = np.select(condition0, choicelist0, default= 99)



pa = ['double','double_play','field_error','field_out','fielders_choice_out','force_out','grounded_into_double_play','hit_by_pitch','home_run','sac_bunt','sac_fly','sac_fly_double_play','single','strikeout','strikeout_double_play','triple','walk']
ab = ['double','double_play','field_error','field_out','fielders_choice_out','force_out','grounded_into_double_play','home_run','single','strikeout','strikeout_double_play','triple']
hit = ['double','home_run','single','triple']
swing = ['foul','foul_tip','hit_into_play','hit_into_play_no_out','hit_into_play_score','swinging_strike','swing_strike_blocked']
con = ['foul','foul_tip','hit_into_play','hit_into_play_no_out','hit_into_play_score']
whiff = ['swinging_strike','swing_strike_blocked']
foul = ['foul','foul_tip']

df['pa'] = df['events'].apply(lambda x: 1 if x in pa else None)
df['ab'] = df['events'].apply(lambda x: 1 if x in ab else None)
df['hit'] = df['events'].apply(lambda x: 1 if x in hit else None)
df['swing'] = df['description'].apply(lambda x: 1 if x in swing else None)
df['con'] = df['description'].apply(lambda x: 1 if x in con else None)
df['whiff'] = df['description'].apply(lambda x: 1 if x in whiff else None)
df['foul'] = df['description'].apply(lambda x: 1 if x in foul else None)

df['z_in'] = df['zone'].apply(lambda x: 1 if x < 10 else None)
df['z_out'] = df['zone'].apply(lambda x: 1 if x > 10 else None)

df['balls'] = df['balls'].apply(str)
df['strikes'] = df['strikes'].apply(str)
df['count'] = df['balls'].str.cat(df['strikes'], sep='-')

speed_fac = 1.609344
distance_fac = 0.3048

df['rel_speed(km)'] = df['release_speed']*speed_fac
df['exit_speed(km)'] = df['launch_speed']*speed_fac
df['rel_height'] = df['release_pos_z']*distance_fac
df['rel_side'] = df['release_pos_x']*distance_fac
df['extension'] = df['release_extension']*distance_fac
df['hit_distance'] = df['hit_distance_sc']*distance_fac

p_type = df[['pitcher','rel_height']]
rel_height = p_type.groupby(['pitcher'], as_index=False).mean()

rel_height['type'] = rel_height['rel_height'].apply(lambda x: 'S' if x <= 1.5 else 'R')
side_arm = rel_height[rel_height['type'] == 'S']
y = side_arm['pitcher'].unique()

df['p_type'] = df['pitcher'].apply(lambda x: 'S' if x in y else 'R')



def pkind(x):

  if x == '4-Seam Fastball':
    return 'Fastball'
  elif x == '2-Seam Fastball':
    return 'Fastball'
  elif x == 'Cutter':
    return 'Fastball'
  elif x == 'Slider':
    return 'Breaking'
  elif x == 'Curveball':
    return 'Breaking'
  elif x == 'Sweeper':
    return 'Breaking'
  elif x == 'Changeup':
    return 'Off_Speed'
  elif x == 'Split-Finger':
    return 'Off_Speed'
  else:
    return 'OT'


df['p_kind'] = df['pitch_name'].apply(lambda x: pkind(x))

def count(x):

  if x == '0-0':
    return 'Neutral'
  elif x == '3-2':
    return 'Neutral'
  elif x == '0-1':
    return 'Pitcher'
  elif x == '0-2':
    return 'Pitcher'
  elif x == '1-1':
    return 'Pitcher'
  elif x == '1-2':
    return 'Pitcher'
  elif x == '2-2':
    return 'Pitcher'
  elif x == '1-0':
    return 'Hitter'
  elif x == '2-0':
    return 'Hitter'
  elif x == '2-1':
    return 'Hitter'
  elif x == '3-0':
    return 'Hitter'
  elif x == '3-1':
    return 'Hitter'

df['count_value'] = df['count'].apply(lambda x: count(x))

df['after_2s'] = df['count_value'].apply(lambda x: 1 if x == 'After_2S' else None)
df['hitting'] = df['count_value'].apply(lambda x: 1 if x == 'Hitting' else None)
df['else'] = df['count_value'].apply(lambda x: 1 if x == 'Else' else None)

df['ld'] = df['bb_type'].apply(lambda x: 1 if x == 'Line_Drive' else None)
df['fb'] = df['bb_type'].apply(lambda x: 1 if x == 'Fly_Ball' else None)
df['gb'] = df['bb_type'].apply(lambda x: 1 if x == 'Ground_Ball' else None)
df['pu'] = df['bb_type'].apply(lambda x: 1 if x == 'Popup' else None)

df['single'] = df['events'].apply(lambda x: 1 if x == 'single' else None)
df['double'] = df['events'].apply(lambda x: 1 if x == 'double' else None)
df['triple'] = df['events'].apply(lambda x: 1 if x == 'triple' else None)
df['home_run'] = df['events'].apply(lambda x: 1 if x == 'home_run' else None)
df['walk'] = df['events'].apply(lambda x: 1 if x == 'walk' else None)
df['strkeout'] = df['events'].apply(lambda x: 1 if x == 'strkeout' else None)
df['hit_by_pitch'] = df['events'].apply(lambda x: 1 if x == 'hit_by_pitch' else None)
df['sac_fly'] = df['events'].apply(lambda x: 1 if x == 'sac_fly' else None)
df['sac_bunt'] = df['events'].apply(lambda x: 1 if x == 'sac_bunt' else None)
df['field_out'] = df['events'].apply(lambda x: 1 if x == 'field_out' else None)

df['inplay'] = df['type'].apply(lambda x: 1 if x == 'X' else None)

df['weak'] = df['launch_speed_angle'].apply(lambda x: 1 if x == 1 else None)
df['topped'] = df['launch_speed_angle'].apply(lambda x: 1 if x == 2 else None)
df['under'] = df['launch_speed_angle'].apply(lambda x: 1 if x == 3 else None)
df['flare'] = df['launch_speed_angle'].apply(lambda x: 1 if x == 4 else None)
df['solid_contact'] = df['launch_speed_angle'].apply(lambda x: 1 if x == 5 else None)
df['barrel'] = df['launch_speed_angle'].apply(lambda x: 1 if x == 6 else None)
df['plus_lsa4'] = df['launch_speed_angle'].apply(lambda x: 1 if x >=4 else None)
df['cs'] = df['description'].apply(lambda x: 1 if x == 'called_strike' else None)


df['game_date'] = pd.to_datetime(df['game_date'], format='mixed')

condition1 = [
             (df['zone'] == 1) & (df['swing'] != 1),
             (df['zone'] == 3) & (df['swing'] != 1),
             (df['zone'] == 4) & (df['swing'] != 1),
             (df['zone'] == 6) & (df['swing'] != 1),
             (df['zone'] == 7) & (df['swing'] != 1),
             (df['zone'] == 9) & (df['swing'] != 1),
]

choicelist1 = ['Left_take','Right_take', 'Left_take', 'Right_take', 'Left_take', 'Right_take']

df['l_r'] = np.select(condition1, choicelist1, default='Not Specified')

condition2 = [
             (df['zone'] == 1) & (df['swing'] != 1),
             (df['zone'] == 2) & (df['swing'] != 1),
             (df['zone'] == 3) & (df['swing'] != 1),
             (df['zone'] == 7) & (df['swing'] != 1),
             (df['zone'] == 8) & (df['swing'] != 1),
             (df['zone'] == 9) & (df['swing'] != 1),
]

choicelist2 = ['High_take','High_take', 'High_take', 'Low_take', 'Low_take', 'Low_take']

df['h_l'] = np.select(condition2, choicelist2, default='Not Specified')

df['z_left'] = df['zone'].apply(lambda x: 1 if x == 1 or x == 4 or x== 7 else None)
df['z_right'] = df['zone'].apply(lambda x: 1 if x == 3 or x == 6 or x== 9 else None)
df['z_high'] = df['zone'].apply(lambda x: 1 if x == 1 or x == 2 or x== 3 else None)
df['z_low'] = df['zone'].apply(lambda x: 1 if x == 7 or x == 8 or x== 9 else None)







condition3 = [
             (df['plate_x'] > -0.381) & (df['plate_x'] < -0.161) & (df['plate_z'] > df['corehigh']) & (df['plate_z'] < df['zonehigh']),
             (df['plate_x'] > -0.161) & (df['plate_x'] < 0.161) & (df['plate_z'] > df['corehigh']) & (df['plate_z'] < df['zonehigh']),
             (df['plate_x'] > 0.161) & (df['plate_x'] < 0.381) & (df['plate_z'] > df['corehigh']) & (df['plate_z'] < df['zonehigh']),
             (df['plate_x'] > -0.381) & (df['plate_x'] < -0.161) & (df['plate_z'] > df['corelow']) & (df['plate_z'] < df['corehigh']),
             (df['plate_x'] > -0.161) & (df['plate_x'] < 0.161) & (df['plate_z'] > df['corelow']) & (df['plate_z'] < df['corehigh']),
             (df['plate_x'] > 0.161) & (df['plate_x'] < 0.381) & (df['plate_z'] > df['corelow']) & (df['plate_z'] < df['corehigh']),
             (df['plate_x'] > -0.381) & (df['plate_x'] < -0.161) & (df['plate_z'] > df['zonelow']) & (df['plate_z'] < df['corelow']),
             (df['plate_x'] > -0.161) & (df['plate_x'] < 0.161) & (df['plate_z'] > df['zonelow']) & (df['plate_z'] < df['corelow']),
             (df['plate_x'] > 0.161) & (df['plate_x'] < 0.381) & (df['plate_z'] > df['zonelow']) & (df['plate_z'] < df['corelow']),
]

choicelist3 = ['nz1','nz2', 'nz3', 'nz4', 'core', 'nz6','nz7','nz8','nz9']

df['new_zone'] = np.select(condition3, choicelist3, default='Not Specified')

df['DH'] = df['game_id'].str[-1]


ndf = df[['game_year', 'game_date', 'inning', 'hometeam','home_score', 'awayteam','away_score',
         'pitch_number','balls', 'strikes', 'zone', 'new_zone','stand', 'p_throw', 'p_throws', 'p_type', 'type', 'bb_type','events', 'description', 'hor_break','ver_break','plate_x','plate_z',
         'pitcherteam', 'pitname', 'pitcher','catcher','batterteam', 'batname', 'batter',
         'rel_speed(km)','release_spin_rate', 'release_spin_axis','rel_height', 'rel_side', 'extension','pitch_name', 'p_kind',
         'exit_speed(km)','launch_angle','launch_direction','hit_distance','hit_spin_rate','launch_speed_angle', 'contactX', 'contactY', 'contactZ', 'groundX', 'groundY', 'l_r','h_l',
         'pa', 'ab', 'hit', 'swing', 'con', 'whiff','foul','z_in','z_out','count', 'count_value', 'z_left','z_right','z_high','z_low',
          'ld','fb','gb','pu','single','double','triple','home_run','walk','strkeout','hit_by_pitch','sac_fly','sac_bunt','field_out','inplay',
          'weak','topped','under','flare','solid_contact','barrel','plus_lsa4','level','DH','cs', 'Height', 'high', 'low', '2/3', '1/3', 'zonehigh', 'corehigh', 'corelow', 'zonelow',
          'x0', 'x5', 'x10', 'x15', 'x20', 'x25', 'x30', 'x35', 'x40', 'x45', 'x50',
          'z0', 'z5', 'z10', 'z15', 'z20', 'z25', 'z30', 'z35', 'z40', 'z45', 'z50',
	  'NAME_batter', 'NAME_pitcher'
	  
        ]]

def ntype(x):

  if x == 'called_strike':
    return 'strike'
  elif x == 'ball':
    return 'ball'   
  elif x == 'hit_into_play':
    return 'inplay'  
  elif x == 'hit_into_play_no_out':
    return 'inplay'   
  elif x == 'hit_into_play_score':
    return 'inplay'   
  elif x == 'swinging_strike':
    return 'whiff'   
  elif x == 'swing_strike_blocked':
    return 'whiff'   
  elif x == 'foul':
    return 'foul'   
  elif x == 'hit_by_pitch':
    return 'hit_by_pitch'   

ndf['ntype'] = ndf['description'].apply(lambda x: ntype(x))

z_df = ndf[ndf['zone'] < 10]
z_df['z_swing'] = z_df['swing'].apply(lambda x: 1 if x == 1 else None)
z_df['z_con'] = z_df['con'].apply(lambda x: 1 if x == 1 else None)
z_df['z_inplay'] = z_df['inplay'].apply(lambda x: 1 if x == 1 else None)

z_swing = z_df[['z_swing']]
z_con = z_df[['z_con']]
z_inplay = z_df[['z_inplay']]

o_df = ndf[ndf['zone'] > 10]
o_df['o_swing'] = o_df['swing'].apply(lambda x: 1 if x == 1 else None)
o_df['o_con'] = o_df['con'].apply(lambda x: 1 if x == 1 else None)
o_df['o_inplay'] = o_df['inplay'].apply(lambda x: 1 if x == 1 else None)

o_swing = o_df[['o_swing']]
o_con = o_df[['o_con']]
o_inplay = o_df[['o_inplay']]

f_pitch = ndf[ndf['count'] == '0-0']
f_pitch['f_swing'] = f_pitch['swing'].apply(lambda x: 1 if x == 1 else None)
f_swing = f_pitch[['f_swing']]

inplay_df = ndf[ndf['type'] == 'X']
inplay_df = inplay_df[['exit_speed(km)','launch_angle','hit_spin_rate']]
inplay_df.columns = ['exit_velocity','launch_angleX','hit_spin']

whiff = ndf[ndf['whiff'] == 1]
whiff['z_str_swing'] = whiff['zone'].apply(lambda x: 1 if x < 10 else None)
z_ztr_swing = whiff[['z_str_swing']]

ndf = ndf.join(z_swing, how='outer')
ndf = ndf.join(o_swing, how='outer')
ndf = ndf.join(z_con, how='outer')
ndf = ndf.join(o_con, how='outer')
ndf = ndf.join(f_swing, how='outer')
ndf = ndf.join(inplay_df, how='outer')
ndf = ndf.join(z_ztr_swing, how='outer')
ndf = ndf.join(z_inplay, how='outer')
ndf = ndf.join(o_inplay, how='outer')

ndf['f_pitch'] = ndf['count'].apply(lambda x: 1 if x == '0-0' else None)
ndf['S'] = np.where(ndf['type'].isin(['S', 'X']), 1, 0)

ndf['Left_take'] = ndf['l_r'].apply(lambda x: 1 if x == 'Left_take' else None)
ndf['Right_take'] = ndf['l_r'].apply(lambda x: 1 if x == 'Right_take' else None)
ndf['High_take'] = ndf['h_l'].apply(lambda x: 1 if x == 'High_take' else None)
ndf['Low_take'] = ndf['h_l'].apply(lambda x: 1 if x == 'Low_take' else None)

ndf['looking'] = ndf['description'].apply(lambda x: 1 if x == "ball" or x == "called_strike" else None)

ot = ndf[ndf['p_kind'] == 'OT'].index
ndf = ndf.drop(ot)

ndf['z_in'].fillna(0, inplace=True)

ndf['month'] = ndf['game_date'].dt.month


# Parquet 파일로 저장 (GitHub Actions에서 실행되는 디렉토리)
ndf.to_parquet("Minor.parquet", index=True, engine="pyarrow")  # 또는 engine="fastparquet"

print("✅ Parquet 데이터 생성 완료")
