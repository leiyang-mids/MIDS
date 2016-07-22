import psycopg2, time
import numpy as np

def get_click_data():
    '''
    '''

    page_interval = 1.5
    conn = psycopg2.connect(database="aca_db", user="postgres", password="pass", host="localhost", port="5432")
    cur = conn.cursor()

    sql_click = '(select c.session_id as sid, string_agg(c.click, \'|\') as c_t from (select session_id, plan_id || \',\' || created_clicks as click from Clicks) c) c2'
    sql_rank = '(select r.session_id as sid, string_agg(r.rank, \'|\') as r_t from (select session_id, plan_id || \',\' || created_ranks || \',\' || plan_score as rank from Ranks) r) r2'
    sql_exe = 'select q.session_id as sid, q.state as state, q.health as health, c2.c_t as click, r2.r_t as rank from \
               Queries q join %s on q.session_id=c2.sid join %s on c2.sid=r2.sid' %(sql_click, sql_rank)
    cur.execute(sql_exe)
    records = cur.fetchall()
    # define data type
    plan_type = [('plan_id',str), ('epoch',float), ('score',float)]
    click_type = [('plan_id',str), ('epoch',float)]
    click_rtn = []
    for sid, state, health, click, rank in records:
        # get plan ranks and clicks, and parse out timestamp
        plans, clicks = [], []
        for r in rank.split('|'):
            _r = r.split(',')
            _r[1] = time.mktime(time.strptime(_r[1],'%Y-%m-%d %H:%M:%S.%f'))
            plans.append(tuple(_r))
        for c in clicks.split('|'):
            _c = c.split(',')
            _c[1] = time.mktime(time.strptime(_c[1],'%Y-%m-%d %H:%M:%S.%f'))
            clicks.append(tuple(_c))
        # sort timestamp to get click order
        p_click = [c['plan_id'] for c in np.sort(np.array(clicks, dtype=click_type), order=['epoch'])]
        # check timestamp block for page, then sort on score to get plan order
        plans = np.sort(np.array(plans, dtype=plan_type), order=['epoch'])
        pages, p_rank = np.zeros(len(plans)), []
        for i in range(1, len(plans)):
            pages[i] = pages[i-1] if plans[i]['epoch']-plans[i-1]['epoch']<page_interval else pages[i-1]+1
        for pg in np.unique(pages):
            p_rank += [pl['plan_id'] for pl in np.sort(plans[pages==pg], order=['score','epoch'])[::-1]] # epoch order in question
        # assemble the query info
        click_rtn.append((state, health, p_rank, p_click))
    # close connection and return
    cur.close()
    conn.close()
    rtn_type = [('state',str), ('query',str), ('ranks',list), ('clicks',list)]
    return np.array(click_rtn, dtype=rtn_type)
