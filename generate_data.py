import os
import pymysql
import pandas as pd
import numpy as np


db = pymysql.connect(host='14.49.30.59', port = 33067, user = 'ktwiz', passwd = 'ktwiz1234!#', db = 'ktwiz')


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

-- game year
-- gameid Like '2020%'
substring(gameid,1,4) >= '2024'
and gameid <> '20241001-Suwon-1'
and ((gameid >='20180401' and gameid <='20181018')
or (gameid >='20190323' and gameid <='20191001')    
or (gameid >='20200504' and gameid <='20201100')
or (gameid >='20210402' and gameid <='20211100')
or (gameid >='20220401' and gameid <='20221012')
or (gameid >='20230401' and gameid <='20231018')
or (gameid >='20240323' and gameid <='20241002')
or (gameid >='20250321')
)



-- league(aaa, KoreaBaseballOrganization, npb, mlb, aa)
AND level IN ('KoreaBaseballOrganization')

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


df = pd.merge(left = df , right = height_df, how = "left", left_on = ["game_year","batter"], right_on = ["game_year", "TM_ID"] )
