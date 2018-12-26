from impala.dbapi import connect as _connect
import psycopg2
from impala.util import as_pandas
from os.path import isfile as _isfile
import time as _time_module
from time import time as _time
from datetime import datetime as _dt

import keyring
import socket

#PARAMS
host = '172.27.0.130' 
port = 21050
database = 'main_day'
auth_mechanism = 'PLAIN'
user = 'barannikov-k'
password_impala = keyring.get_password('ldap', 'barannikov-k')
password_redshift = keyring.get_password('redshift', 'barannikov')
use_ssl = False

def use_ip(new_ip):
    global ip
    ip = new_ip

use_ip('172.27.0.235')    

def use_amazon_balancer():
    use_ip('impala.service.aav01.consul')

def use_amazon_stage_balancer():
    use_ip('impala.service.asv01.consul')

def _current_time():
    return _dt.strftime(_dt.now(), '%H:%M:%S')

def _log_file(name):
    print('{}'.format(name))

def _log_time(name):
    print('{} at {}'.format(name, _current_time()))
    
def _log_tracking_url():
    try:
        print('http://{}:25000/queries'.format(socket.gethostbyname_ex('172.27.0.235')[2][0]), end=' ') #print('http://{}:25000/queries'.format(socket.gethostbyname_ex('impala.playnet.local')[2][0]), end=' ') 
    except IndexError:        
        print('couldnt get tracking url, use Cloudera Manager: http://aws-an-impala-01.playnet.local:7180/cmf/services/14/queries', end=' ')

def execute_rs_df(sql): # Amazon Redshift connection
    rows = None
    with psycopg2.connect('host=172.25.0.93 port=5439 dbname=plr user=barannikov password={}'.format(password_redshift)) as conn:        
        cur = conn.cursor()
        cur.execute(sql)
        try:
            return as_pandas(cur) 
        except psycopg2.ProgrammingError:
            return 'no result'
        print('sql ready')

def execute(sql):
    rows = None
    _log_time('start')
    cur_time = _time()
    
    print('loading data', end=' ')
    with _connect(ip, auth_mechanism=auth_mechanism, user=user, password=password_impala) as conn:
        _log_tracking_url()
        cursor = conn.cursor()
        cursor.execute(sql)
        if cursor.has_result_set:
            rows = cursor.fetchall()
    
    duration = round(_time() - cur_time)
    if duration > 0:
        print('...done for {} sec'.format(duration))
    return rows

def execute_df(sql):
    """
    Execute query and return result as DataFrame
    :rtype DataFrame
    """
    _log_time('start')
    cur_time = _time()
    
    print('loading data', end=' ')    
    with _connect(ip, auth_mechanism=auth_mechanism, user=user, password=password_impala) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        if cur.has_result_set:        
            df = as_pandas(cur)

    duration = round(_time() - cur_time)
    if duration > 0:
        print('...done for {} sec'.format(duration))
    return df

'''
def execute_df(sql):
    """
    Execute query and return result as DataFrame
    :rtype DataFrame
    """
    _log_time('start')
    cur_time = _time()
    
    print('loading data', end=' ')    
    with _connect(ip, auth_mechanism=auth_mechanism, user=user, password=password_impala) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        return as_pandas(cur)

    duration = round(_time() - cur_time)
    if duration > 0:
        print('...done for {} sec'.format(duration))
    return rows
        
'''
        

def execute_df_bad_impala(sql, k):
    i = 0
    while i < k:  
        try:
            _df = execute_df(sql)
        except Exception:
            i += 1
            _time_module.sleep(i * 5)
            print('Не взлетело. Еще разок' + str(i))   
        else:
            return _df

def execute_to_file(filename, sql):
    _log_file(filename)
    _log_time('start')
    file_mode = _inspect_file_mode(filename)
    if (file_mode == False):
        return
    
    with open(filename, file_mode, encoding='utf-8') as file:
        cur_time = _time()
        print('loading data', end=' ')
        _execute_add_to_file(file, sql, True)
        duration = round(_time() - cur_time)
        if duration > 0:
            print('...done for {} sec'.format(duration))

def _execute_add_to_file(file, sql, useBlocks = False):
    with _connect(ip, auth_mechanism=auth_mechanism, user=user, password=password_impala) as conn:
        _log_tracking_url()
        cursor = conn.cursor()
        cursor.execute(sql)
        
        if useBlocks:
            block_size = 100000
            block = cursor.fetchmany(size=block_size)
            while block:
                for row in block:
                    row = [str(item) for item in row]
                    try:
                        file.write('%s\n' % ';'.join(row))
                    except Exception:
                        print(row)
                        continue
                block = cursor.fetchmany(size=block_size)
                
        else:
            rows = cursor.fetchall()
            for row in rows:
                row = [str(item) for item in row]
                try:
                    file.write('%s\n' % ';'.join(row))
                except TypeError:
                    print(row)
                    continue

def _file_exists_prompt():
    message = 'File already exists - rewrite [w] or append [a] or exit [x]? '
    while True:
        print(message)
        choice = input().lower()
        if choice == 'w':
            return 'w'
        elif choice == 'a':
            return 'a'
        elif choice == 'x':
            return 'x'

def _inspect_file_mode(filename):
    file_mode = 'w'
    if _isfile(filename):
        prompt_result = _file_exists_prompt()
        if prompt_result == 'x':
            return False
        else:    
            file_mode = prompt_result
    return file_mode

def execute_day_series_to_file(filename, days, field, sql, config = None):
    dict_configs = []
    for day in days:
        dict_configs.append({field: str(day)})
    
    execute_dict_series_to_file(filename, dict_configs, sql, config)
    
def execute_dict_series_to_file(filename, dict_configs, sql, config = None):
    _log_file(filename)
    _log_time('start')
    file_mode = _inspect_file_mode(filename)
    if (file_mode == False):
        return
    
    with open(filename, file_mode, encoding='utf-8') as file:
        cur_num = 0
        all_num = len(dict_configs)
        durations = []
        for map_config in dict_configs:
            cur_num += 1
            cur_time = _time()
            
            estimation = 0 if cur_num == 1 else round((sum(durations)/len(durations) * (all_num - cur_num)) / 60)
            
            print('loading data {}/{} {} timeleft: {} min'.format(cur_num, all_num, map_config, estimation), end=' ')
            if config is not None:
                map_config = {**map_config, **config}
                
            _execute_add_to_file(file, sql.format_map(map_config))
            
            duration = round(_time() - cur_time)
            durations.append(duration)
            if duration > 0:
                print('...done for {} sec'.format(duration))
        print('done all for {} min!\n'.format(round(sum(durations)/60)))
            
            

