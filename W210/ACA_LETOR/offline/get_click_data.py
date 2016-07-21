import psycopg2, time

def get_click_data():
    '''
    '''

    conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost", port="5432")
    cur = conn.cursor()

    sql_click = '(select c.session_id as sid, string_agg(c.click, \'|\') as c_t from (select session_id, plan_id || \',\' || created_clicks as click from Clicks) c) c2'
    sql_rank = '(select r.session_id as sid, string_agg(r.rank, \'|\') as r_t from (select session_id, plan_id || \',\' || created_ranks as rank from Ranks) r) r2'
    sql_exe = 'select q.session_id as sid, q.state as state, q.health as health, c2.c_t as click, r2.r_t as rank from \
               Queries q join %s on q.session_id=c2.sid join %s on c2.sid=r2.sid' %(sql_click, sql_rank)
    cur.execute(sql_exe)
    records = cur.fetchall()
    for sid, state, health, click, rank in records:
        # get plan ranks
        plans = [[pid, time.strptime(t) for pid,t in p.split(',')] for p in rank.split('|')]
        # time.mktime(time.strptime(p.split(',')[1],'%Y-%m-%d %H:%M:%S.%f'))

    cur.close()
    conn.close()
