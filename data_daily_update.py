import os
import sys
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from io import BytesIO
import pymysql


# ✅ GitHub 정보 설정
OWNER = "Henryjeon1"
REPO = "Trackman"
TOKEN = "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"

# ✅ 환경 변수에서 날짜 가져오기
TARGET_DATE = os.getenv("TARGET_DATE")

# ✅ 만약 환경 변수가 없으면 기본값을 입력받음
if not TARGET_DATE:
    if len(sys.argv) > 1:
        TARGET_DATE = sys.argv[1]
    else:
        TARGET_DATE = input("업데이트할 날짜를 입력하세요 (예: YYYY-MM-DD): ")

print(f"📅 업데이트할 날짜: {TARGET_DATE}")

# ✅ 항상 'latest' 릴리스에서 CSV 다운로드
release_url = f"https://api.github.com/repos/{OWNER}/{REPO}/releases/latest"
headers = {"Authorization": f"token {TOKEN}"}

response = requests.get(release_url, headers=headers)

if response.status_code == 200:
    release_data = response.json()
    
    for asset in release_data.get('assets', []):
        if asset['name'] == "data.csv":
            # assets API URL 사용
            asset_url = asset['url']
            print(f"\nAsset API URL: {asset_url}")
            
            # API 요청 헤더 수정
            download_headers = {
                'Authorization': f'token {TOKEN}',
                'Accept': 'application/octet-stream'  # 중요: 이 헤더를 추가
            }
            
            # assets API를 통해 파일 다운로드
            response = requests.get(
                asset_url,
                headers=download_headers,
                allow_redirects=True
            )
            
            print(f"파일 다운로드 상태: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    df = pd.read_csv(BytesIO(response.content))

                except Exception as e:
                    print(f"데이터프레임 생성 중 오류: {str(e)}")
            else:
                print(f"다운로드 실패: {response.text}")
            break

    # ✅ 기존 데이터에서 TARGET_DATE 삭제
    df = df[df["game_date"] != TARGET_DATE]

    # ✅ SQL에서 새로운 TARGET_DATE 데이터 가져오기  
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
    substring(gameid,1,4) >= '2020'
    and gameid <> '20241001-Suwon-1'
    AND STR_TO_DATE(`DATE`, '%Y-%m-%d') = STR_TO_DATE('{target_date}', '%Y-%m-%d')  -- ✅ 여기에 날짜 필터링 추가



    -- league(aaa, KoreaBaseballOrganization, npb, mlb, aa)
    AND level IN ('KoreaBaseballOrganization')

    order by gameid, pitchno 

    """



    cursor.execute(sql)
    raw = cursor.fetchall()

    ddf=pd.DataFrame(raw, columns = ['game_id','pitch_type', 'game_date', 'release_speed', 'release_pos_x', 'release_pos_z',  'pitname', 'batname', 'batter', 'pitcher', 'events', 'description', 'zone', 'des', 'stand', 'p_throw', 'pitcherteam','batterteam', 'hometeam' , 'awayteam',
                                    'type', 'bb_type', 'balls', 'strikes', 'pfx_x', 'pfx_z', 'plate_x', 'plate_z', 'out_when_up', 'inning', 'inning_topbot', 'hit_distance_sc',
                                    'launch_speed','launch_angle','release_spin_rate','release_spin_axis', 'release_extension',
                                    'launch_speed_angle','pitch_number','PAofinning','pitch_name','home_score','away_score','level','verrelangle','launch_direction', 'contactX' , 'contactY' , 'contactZ', 'groundX','groundY','game_year','hit_spin_rate', 'catcher',
                                    'x0', 'x5', 'x10', 'x15', 'x20', 'x25', 'x30', 'x35', 'x40', 'x45', 'x50',
                                    'z0', 'z5', 'z10', 'z15', 'z20', 'z25', 'z30', 'z35', 'z40', 'z45', 'z50'])

    ddf['plate_x'] = ddf['plate_x'] * -1.0
    ddf['pfx_x'] = ddf['pfx_x'] * -1.0


    cursor = db.cursor()
    height_sql = """
    select * from player_info  """

    cursor.execute(height_sql)
    height_raw = cursor.fetchall()

    height_df=pd.DataFrame(height_raw, columns = ['game_year', 'team', 'team_tm_major', 'team_tm_minor','KBO_ID','TM_ID','NAME','POS','BackNum','engName','Birthday','Height','Weight','PitcherThrows','BatterSide'])

    height_df['TM_ID'] = height_df['TM_ID'].astype(str)
    height_df['game_year'] = height_df['game_year'].astype(str)


    ddf = pd.merge(left = ddf , right = height_df, how = "left", left_on = ["game_year","batter"], right_on = ["game_year", "TM_ID"] )



    ddf[['batter','pitcher','groundX','groundY','game_year']] = ddf[['batter','pitcher','groundX', 'groundY','game_year']].apply(pd.to_numeric)

    conv_fac = 0.3048

    ddf['rel_height'] = ddf['release_pos_z']*conv_fac
    ddf['rel_side'] = ddf['release_pos_x']*conv_fac
    ddf['hor_break'] = ddf['pfx_x']*conv_fac*100
    ddf['ver_break'] = ddf['pfx_z']*conv_fac*100

    p_type = ddf[['pitcher','rel_height']]
    rel_height = p_type.groupby(['pitcher'], as_index=False).mean()
    rel_height['type'] = rel_height['rel_height'].apply(lambda x: 'S' if x <= 1.5 else 'R')
    side_arm = rel_height[rel_height['type'] == 'S']

    y = side_arm['pitcher'].unique()

    ddf['side_arm'] = ddf['pitcher'].apply(lambda x: 'S' if x in y else 'Reg')
    ddf['p_throws'] = ddf['pitcher'].apply(lambda x: 'S' if x in y else (list(set(ddf.loc[ddf['pitcher'] == x, 'p_throw'].unique()))+[None])[0])

    ddf['Height'] = pd.to_numeric(ddf['Height'], errors='coerce')

    # 신장 가져오기
    ddf['high'] = np.where(ddf['Height'].isnull(), 1.049 , (ddf['Height'] * 0.5575 + 3.5) / 100)
    ddf['low']  = np.where(ddf['Height'].isnull(), 0.463 , (ddf['Height'] * 0.2704 - 3.5) / 100)
    # 신장이 없는 경우는 일단 183cm 기준으로 가져오는 것으로 함 


    ddf['1/3'] = (ddf['low'] + ((ddf['high'] - ddf['low']) / 3))
    ddf['2/3'] = (ddf['high'] - ((ddf['high'] - ddf['low']) / 3))

    # ddf['zonehigh'] = ddf['Height'] * 0.6335 / 100
    # ddf['corehigh'] = ddf['Height'] * 0.4935 / 100
    # ddf['corelow'] = ddf['Height'] * 0.3464 / 100
    # ddf['zonelow'] = ddf['Height'] * 0.2064 / 100


    ddf['zonehigh'] = ddf['high'] + 0.11
    ddf['corehigh'] = ddf['high'] - 0.11
    ddf['corelow'] = ddf['low'] + 0.11
    ddf['zonelow'] = ddf['low'] - 0.11

    condition9 = [
                (ddf['plate_x'] > -0.271) & (ddf['plate_x'] < 0.271) & (ddf['plate_z'] > ddf['low'] ) & (ddf['plate_z'] < ddf['high']),
                (ddf['plate_x'] > -0.381) & (ddf['plate_x'] < 0.381) & (ddf['plate_z'] > ddf['zonelow']) & (ddf['plate_z'] < ddf['zonehigh'])
                
    ]

    choicelist9 = ['IN', 'OUT']

    ddf['INOUT'] = np.select(condition9, choicelist9, default= 'Not Specified')

    condition0 = [
                (ddf['plate_x'] > -0.271) & (ddf['plate_x'] < -0.0903) & (ddf['plate_z'] > ddf['2/3']) & (ddf['plate_z'] < ddf['high']),
                (ddf['plate_x'] > -0.0903) & (ddf['plate_x'] < 0.0903) & (ddf['plate_z'] > ddf['2/3']) & (ddf['plate_z'] < ddf['high']),
                (ddf['plate_x'] > 0.0903) & (ddf['plate_x'] < 0.271) & (ddf['plate_z'] > ddf['2/3']) & (ddf['plate_z'] < ddf['high']),
                
                (ddf['plate_x'] > -0.271) & (ddf['plate_x'] < -0.0903) & (ddf['plate_z'] > ddf['1/3']) & (ddf['plate_z'] < ddf['2/3']),
                (ddf['plate_x'] > -0.0903) & (ddf['plate_x'] < 0.0903) & (ddf['plate_z'] > ddf['1/3']) & (ddf['plate_z'] < ddf['2/3']),
                (ddf['plate_x'] > 0.0903) & (ddf['plate_x'] < 0.271) & (ddf['plate_z'] > ddf['1/3']) & (ddf['plate_z'] < ddf['2/3']),
                
                (ddf['plate_x'] > -0.271) & (ddf['plate_x'] < -0.0903) & (ddf['plate_z'] > ddf['low']) & (ddf['plate_z'] < ddf['1/3']),
                (ddf['plate_x'] > -0.0903) & (ddf['plate_x'] < 0.0903) & (ddf['plate_z'] > ddf['low']) & (ddf['plate_z'] < ddf['1/3']),
                (ddf['plate_x'] > 0.0903) & (ddf['plate_x'] < 0.271) & (ddf['plate_z'] > ddf['low']) & (ddf['plate_z'] < ddf['1/3']),
                
                (ddf['INOUT'] == 'OUT') & (ddf['plate_x'] < 0) & (ddf['plate_z'] > 0.75) ,
                (ddf['INOUT'] == 'OUT') & (ddf['plate_x'] > 0) & (ddf['plate_z'] > 0.75) ,
                (ddf['INOUT'] == 'OUT') & (ddf['plate_x'] < 0) & (ddf['plate_z'] < 0.75) ,
                (ddf['INOUT'] == 'OUT') & (ddf['plate_x'] > 0) & (ddf['plate_z'] < 0.75) ,
                
                
    ]

    choicelist0 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14]

    ddf['zone'] = np.select(condition0, choicelist0, default= 99)



    pa = ['double','double_play','field_error','field_out','fielders_choice_out','force_out','grounded_into_double_play','hit_by_pitch','home_run','sac_bunt','sac_fly','sac_fly_double_play','single','strikeout','strikeout_double_play','triple','walk']
    ab = ['double','double_play','field_error','field_out','fielders_choice_out','force_out','grounded_into_double_play','home_run','single','strikeout','strikeout_double_play','triple']
    hit = ['double','home_run','single','triple']
    swing = ['foul','foul_tip','hit_into_play','hit_into_play_no_out','hit_into_play_score','swinging_strike','swing_strike_blocked']
    con = ['foul','foul_tip','hit_into_play','hit_into_play_no_out','hit_into_play_score']
    whiff = ['swinging_strike','swing_strike_blocked']
    foul = ['foul','foul_tip']

    ddf['pa'] = ddf['events'].apply(lambda x: 1 if x in pa else None)
    ddf['ab'] = ddf['events'].apply(lambda x: 1 if x in ab else None)
    ddf['hit'] = ddf['events'].apply(lambda x: 1 if x in hit else None)
    ddf['swing'] = ddf['description'].apply(lambda x: 1 if x in swing else None)
    ddf['con'] = ddf['description'].apply(lambda x: 1 if x in con else None)
    ddf['whiff'] = ddf['description'].apply(lambda x: 1 if x in whiff else None)
    ddf['foul'] = ddf['description'].apply(lambda x: 1 if x in foul else None)

    ddf['z_in'] = ddf['zone'].apply(lambda x: 1 if x < 10 else None)
    ddf['z_out'] = ddf['zone'].apply(lambda x: 1 if x > 10 else None)

    ddf['balls'] = ddf['balls'].apply(str)
    ddf['strikes'] = ddf['strikes'].apply(str)
    ddf['count'] = ddf['balls'].str.cat(ddf['strikes'], sep='-')

    speed_fac = 1.609344
    distance_fac = 0.3048

    ddf['rel_speed(km)'] = ddf['release_speed']*speed_fac
    ddf['exit_speed(km)'] = ddf['launch_speed']*speed_fac
    ddf['rel_height'] = ddf['release_pos_z']*distance_fac
    ddf['rel_side'] = ddf['release_pos_x']*distance_fac
    ddf['extension'] = ddf['release_extension']*distance_fac
    ddf['hit_distance'] = ddf['hit_distance_sc']*distance_fac

    p_type = ddf[['pitcher','rel_height']]
    rel_height = p_type.groupby(['pitcher'], as_index=False).mean()

    rel_height['type'] = rel_height['rel_height'].apply(lambda x: 'S' if x <= 1.5 else 'R')
    side_arm = rel_height[rel_height['type'] == 'S']
    y = side_arm['pitcher'].unique()

    ddf['p_type'] = ddf['pitcher'].apply(lambda x: 'S' if x in y else 'R')



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


    ddf['p_kind'] = ddf['pitch_name'].apply(lambda x: pkind(x))

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

    ddf['count_value'] = ddf['count'].apply(lambda x: count(x))

    ddf['after_2s'] = ddf['count_value'].apply(lambda x: 1 if x == 'After_2S' else None)
    ddf['hitting'] = ddf['count_value'].apply(lambda x: 1 if x == 'Hitting' else None)
    ddf['else'] = ddf['count_value'].apply(lambda x: 1 if x == 'Else' else None)

    ddf['ld'] = ddf['bb_type'].apply(lambda x: 1 if x == 'Line_Drive' else None)
    ddf['fb'] = ddf['bb_type'].apply(lambda x: 1 if x == 'Fly_Ball' else None)
    ddf['gb'] = ddf['bb_type'].apply(lambda x: 1 if x == 'Ground_Ball' else None)
    ddf['pu'] = ddf['bb_type'].apply(lambda x: 1 if x == 'Popup' else None)

    ddf['single'] = ddf['events'].apply(lambda x: 1 if x == 'single' else None)
    ddf['double'] = ddf['events'].apply(lambda x: 1 if x == 'double' else None)
    ddf['triple'] = ddf['events'].apply(lambda x: 1 if x == 'triple' else None)
    ddf['home_run'] = ddf['events'].apply(lambda x: 1 if x == 'home_run' else None)
    ddf['walk'] = ddf['events'].apply(lambda x: 1 if x == 'walk' else None)
    ddf['hit_by_pitch'] = ddf['events'].apply(lambda x: 1 if x == 'hit_by_pitch' else None)
    ddf['sac_fly'] = ddf['events'].apply(lambda x: 1 if x == 'sac_fly' else None)
    ddf['sac_bunt'] = ddf['events'].apply(lambda x: 1 if x == 'sac_bunt' else None)
    ddf['field_out'] = ddf['events'].apply(lambda x: 1 if x == 'field_out' else None)

    ddf['inplay'] = ddf['type'].apply(lambda x: 1 if x == 'X' else None)

    ddf['weak'] = ddf['launch_speed_angle'].apply(lambda x: 1 if x == 1 else None)
    ddf['topped'] = ddf['launch_speed_angle'].apply(lambda x: 1 if x == 2 else None)
    ddf['under'] = ddf['launch_speed_angle'].apply(lambda x: 1 if x == 3 else None)
    ddf['flare'] = ddf['launch_speed_angle'].apply(lambda x: 1 if x == 4 else None)
    ddf['solid_contact'] = ddf['launch_speed_angle'].apply(lambda x: 1 if x == 5 else None)
    ddf['barrel'] = ddf['launch_speed_angle'].apply(lambda x: 1 if x == 6 else None)
    ddf['plus_lsa4'] = ddf['launch_speed_angle'].apply(lambda x: 1 if x >=4 else None)
    ddf['cs'] = ddf['description'].apply(lambda x: 1 if x == 'called_strike' else None)


    ddf['game_date'] = pd.to_datetime(ddf['game_date'], format='mixed')

    condition1 = [
                (ddf['zone'] == 1) & (ddf['swing'] != 1),
                (ddf['zone'] == 3) & (ddf['swing'] != 1),
                (ddf['zone'] == 4) & (ddf['swing'] != 1),
                (ddf['zone'] == 6) & (ddf['swing'] != 1),
                (ddf['zone'] == 7) & (ddf['swing'] != 1),
                (ddf['zone'] == 9) & (ddf['swing'] != 1),
    ]

    choicelist1 = ['Left_take','Right_take', 'Left_take', 'Right_take', 'Left_take', 'Right_take']

    ddf['l_r'] = np.select(condition1, choicelist1, default='Not Specified')

    condition2 = [
                (ddf['zone'] == 1) & (ddf['swing'] != 1),
                (ddf['zone'] == 2) & (ddf['swing'] != 1),
                (ddf['zone'] == 3) & (ddf['swing'] != 1),
                (ddf['zone'] == 7) & (ddf['swing'] != 1),
                (ddf['zone'] == 8) & (ddf['swing'] != 1),
                (ddf['zone'] == 9) & (ddf['swing'] != 1),
    ]

    choicelist2 = ['High_take','High_take', 'High_take', 'Low_take', 'Low_take', 'Low_take']

    ddf['h_l'] = np.select(condition2, choicelist2, default='Not Specified')

    ddf['z_left'] = ddf['zone'].apply(lambda x: 1 if x == 1 or x == 4 or x== 7 else None)
    ddf['z_right'] = ddf['zone'].apply(lambda x: 1 if x == 3 or x == 6 or x== 9 else None)
    ddf['z_high'] = ddf['zone'].apply(lambda x: 1 if x == 1 or x == 2 or x== 3 else None)
    ddf['z_low'] = ddf['zone'].apply(lambda x: 1 if x == 7 or x == 8 or x== 9 else None)







    condition3 = [
                (ddf['plate_x'] > -0.381) & (ddf['plate_x'] < -0.161) & (ddf['plate_z'] > ddf['corehigh']) & (ddf['plate_z'] < ddf['zonehigh']),
                (ddf['plate_x'] > -0.161) & (ddf['plate_x'] < 0.161) & (ddf['plate_z'] > ddf['corehigh']) & (ddf['plate_z'] < ddf['zonehigh']),
                (ddf['plate_x'] > 0.161) & (ddf['plate_x'] < 0.381) & (ddf['plate_z'] > ddf['corehigh']) & (ddf['plate_z'] < ddf['zonehigh']),
                (ddf['plate_x'] > -0.381) & (ddf['plate_x'] < -0.161) & (ddf['plate_z'] > ddf['corelow']) & (ddf['plate_z'] < ddf['corehigh']),
                (ddf['plate_x'] > -0.161) & (ddf['plate_x'] < 0.161) & (ddf['plate_z'] > ddf['corelow']) & (ddf['plate_z'] < ddf['corehigh']),
                (ddf['plate_x'] > 0.161) & (ddf['plate_x'] < 0.381) & (ddf['plate_z'] > ddf['corelow']) & (ddf['plate_z'] < ddf['corehigh']),
                (ddf['plate_x'] > -0.381) & (ddf['plate_x'] < -0.161) & (ddf['plate_z'] > ddf['zonelow']) & (ddf['plate_z'] < ddf['corelow']),
                (ddf['plate_x'] > -0.161) & (ddf['plate_x'] < 0.161) & (ddf['plate_z'] > ddf['zonelow']) & (ddf['plate_z'] < ddf['corelow']),
                (ddf['plate_x'] > 0.161) & (ddf['plate_x'] < 0.381) & (ddf['plate_z'] > ddf['zonelow']) & (ddf['plate_z'] < ddf['corelow']),
    ]

    choicelist3 = ['nz1','nz2', 'nz3', 'nz4', 'core', 'nz6','nz7','nz8','nz9']

    ddf['new_zone'] = np.select(condition3, choicelist3, default='Not Specified')

    ddf['DH'] = ddf['game_id'].str[-1]


    ndf = ddf[['game_year', 'game_date', 'inning', 'hometeam','home_score', 'awayteam','away_score',
            'pitch_number','balls', 'strikes', 'zone', 'new_zone','stand', 'p_throw', 'p_throws', 'p_type', 'type', 'bb_type','events', 'description', 'hor_break','ver_break','plate_x','plate_z',
            'pitcherteam', 'pitname', 'pitcher','catcher','batterteam', 'batname', 'batter',
            'rel_speed(km)','release_spin_rate', 'release_spin_axis','rel_height', 'rel_side', 'extension','pitch_name', 'p_kind',
            'exit_speed(km)','launch_angle','launch_direction','hit_distance','hit_spin_rate','launch_speed_angle', 'contactX', 'contactY', 'contactZ', 'groundX', 'groundY', 'l_r','h_l',
            'pa', 'ab', 'hit', 'swing', 'con', 'whiff','foul','z_in','z_out','count', 'count_value', 'z_left','z_right','z_high','z_low',
            'ld','fb','gb','pu','single','double','triple','home_run','walk','hit_by_pitch','sac_fly','sac_bunt','field_out','inplay',
            'weak','topped','under','flare','solid_contact','barrel','plus_lsa4','level','DH','cs', 'Height', 'high', 'low', '2/3', '1/3', 'zonehigh', 'corehigh', 'corelow', 'zonelow',
            'x0', 'x5', 'x10', 'x15', 'x20', 'x25', 'x30', 'x35', 'x40', 'x45', 'x50',
            'z0', 'z5', 'z10', 'z15', 'z20', 'z25', 'z30', 'z35', 'z40', 'z45', 'z50'
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
    inplay_df = inplay_df[['exit_speed(km)','launch_angle']]
    inplay_df.columns = ['exit_velocity','launch_angleX']

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

    ndf['Left_take'] = ndf['l_r'].apply(lambda x: 1 if x == 'Left_take' else None)
    ndf['Right_take'] = ndf['l_r'].apply(lambda x: 1 if x == 'Right_take' else None)
    ndf['High_take'] = ndf['h_l'].apply(lambda x: 1 if x == 'High_take' else None)
    ndf['Low_take'] = ndf['h_l'].apply(lambda x: 1 if x == 'Low_take' else None)

    ndf['looking'] = ndf['description'].apply(lambda x: 1 if x == "ball" or x == "called_strike" else None)

    ot = ndf[ndf['p_kind'] == 'OT'].index
    ndf = ndf.drop(ot)

    ndf['z_in'].fillna(0, inplace=True)

    ndf['month'] = ndf['game_date'].dt.month

    # ✅ 새로운 데이터 추가
    df = pd.concat([df, ndf], ignore_index=True)

    # ✅ 업데이트된 CSV 다시 저장 & 업로드
    df.to_csv("data.csv", index=False)

    print(f"✅ {TARGET_DATE} 데이터 업데이트 완료!")
else:
    print(f"❌ 릴리스 가져오기 실패 (HTTP {response.status_code})")
