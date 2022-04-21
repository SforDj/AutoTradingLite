from datetime import datetime, timedelta
import time

from algorithm.algo_mac1_mac2 import AlgoMac1Mac2
from dataset.data_connection import DatasetMysql
from entity import security_price
from utils.send_msg import MSGSender

from common.config import jq_userid, jq_passwd

import jqdatasdk as jq

from common.config import g_map
g_securities = g_map.keys()


# init mysql & jq etc.
def init_env():
    handler = DatasetMysql()
    reporter = MSGSender()
    jq.auth(jq_userid, jq_passwd)
    algos = {}
    for code in g_securities:
        algos[code] = AlgoMac1Mac2(code)
    return handler, reporter, algos


# load data for cold start
def prepare_data(handler: DatasetMysql, no_data: bool):
    # if dataset was first start up, theres no data in dataset,
    # so we need to fetch data from jq/or else api and write dataset first.
    if no_data:
        result = fetch_past_data_to_mysql(handler)
        if not result:
            return False, {}

    # as for data push, we need to calc statistics like macX, ema, etc. some of them need past data stream
    # so we first load these past data from dataset for cold start.
    # cold start: when we run program, theres maybe no data the algorithm need, so we need to do some init.
    result, data_cache = load_cold_start_data(handler)
    if not result:
        return False, data_cache

    return True, data_cache


# fetch data use 3rd-party api and dump to dataset (currently jq)
def fetch_past_data_to_mysql(handler: DatasetMysql):
    print("fetch_past_data_to_mysql begin.")
    curr_time = datetime.fromtimestamp(time.time())

    # here the api not support 30-minute paused schema, so get price by per minute
    # time_most_recent = normalize_time_by_level(curr_time, 30)
    time_most_recent = curr_time

    for code in g_securities:
        code_jq_normalized = jq.normalize_code(code)
        price_data = jq.get_price(code_jq_normalized, end_date=time_most_recent, count=20 * 30,
                                  frequency='1m', fields=['open', 'close', 'high', 'low', 'volume', 'money', 'paused'],
                                  panel=False)
        for i in range(len(price_data)):
            sp = security_price.make_from_data_frame(code, price_data.iloc[i])
            sql_txt = sp.gen_insert_sql()
            if not handler.execute_if_exist(sp.code_, sp.time_, sql_txt):
                print("it has exist. no need to insert to dataset")
                # return False
    print("fetch_past_data_to_mysql success.")
    return True


# load past data from dataset for cold start
def load_cold_start_data(handler: DatasetMysql):
    print("load_cold_start_data begin.")
    cache = {}
    for code in g_securities:
        cache[code] = []

    # for test
    curr_time = datetime.fromtimestamp(time.time())
    # curr_time = datetime.strptime("2022-04-06 9:26:00", "%Y-%m-%d %H:%M:%S")

    time_most_recent = normalize_time_by_level(curr_time, 30)

    for code in g_securities:
        i, j = 0, 0

        while i < 20:
            time_query = time_most_recent + timedelta(minutes=-30) * j
            sql_txt = security_price.gen_query_sql(code, time_query)
            print(sql_txt)
            data_from_sql = handler.query(sql_txt)
            if len(data_from_sql) == 1:
                print("find one ***************************************")
                sp = security_price.make_from_sql(data_from_sql[0])
                cache[code].append(sp)
                i += 1
            # else:
                # print("load_cold_start_data failed. error in fetch_sql_price_data_from_now, no or replicate data fetched")
                # return False, {}
            j += 1

        cache[code].reverse()

    print("load_cold_start_data success.")
    return True, cache


def normalize_time_by_level(in_time: datetime, minute_level: int):
    if in_time.minute >= minute_level:
        delta_time = timedelta(days=0, seconds=in_time.second, microseconds=in_time.microsecond,
                               milliseconds=0, minutes=in_time.minute - minute_level, hours=0,
                               weeks=0)
        out_time = in_time - delta_time
    else:
        delta_time = timedelta(days=0, seconds=in_time.second, microseconds=in_time.microsecond,
                               milliseconds=0, minutes=in_time.minute, hours=0,
                               weeks=0)
        out_time = in_time - delta_time

    if 15 < out_time.hour < 9 or (out_time.hour == 9 and out_time.minute < 30):
        delta_time = timedelta(days=-1, seconds=0, microseconds=0,
                               milliseconds=0, minutes=30 - out_time.minute, hours=15 - out_time.hour,
                               weeks=0)
        out_time += delta_time

    return out_time


# loop every day in trading time
def main_loop(cache: dict, reporter: MSGSender, algos: dict, handler: DatasetMysql):
    while True:
        # judge if in trading time
        curr_time = datetime.fromtimestamp(time.time())
        if not in_trading(curr_time):
            print("not trading time")
            time_end = datetime.fromtimestamp(time.time())
            time.sleep(60 - time_end.second)
            continue
        else:
            print("trading...")

        if curr_time.minute < 28 or 30 < curr_time.minute < 58:
            print("no need to do algorithm")
            time_end = datetime.fromtimestamp(time.time())
            time.sleep(60 - time_end.second)
            continue

        # get data for current time point
        new_data = fetch_data_online_by_time(curr_time, handler)

        # merge data (might do sth to pre-process the new data)
        merge_data(cache, new_data)
        # run the algorithm and return the signal, if signal True, we also get the message reporting to users
        signal, msg = do_algorithm(cache, algos)
        if signal:
            report(reporter, msg)

        # trigger next minute request
        time_end = datetime.fromtimestamp(time.time())
        time.sleep(60 - time_end.second)
        # curr_time += delta_time

    return


# judge if in trading time
def in_trading(curr_time: datetime):
    # before 9:00
    if curr_time.hour < 9:
        return False

    # at noon
    if 12 >= curr_time.hour >= 11:
        if curr_time.hour == 11 and curr_time.minute > 30:
            return False
        elif curr_time.hour == 12:
            return False

    # after 15:00
    if curr_time.hour >= 15 and curr_time.minute > 0:
        return False
    # else
    return True


# get data for current time point
def fetch_data_online_by_time(curr_time: time, handler: DatasetMysql):
    print("fetch_data_online_by_time begin")
    data = {}

    for code in g_securities:
        code_jq_normalized = jq.normalize_code(code)
        price_data = jq.get_price(code_jq_normalized, end_date=curr_time, count=1,
                                  frequency='minute', fields=['open', 'close', 'high', 'low', 'volume', 'money', 'paused'],
                                  panel=False)

        sp = security_price.make_from_data_frame(code, price_data.iloc[0])
        data[code] = sp

        sql_txt = sp.gen_insert_sql()
        if not handler.execute_if_exist(sp.code_, sp.time_, sql_txt):
            print("fetch_past_data_to_mysql failed. met error in execute sql")
            # return {}

    print("fetch_data_online_by_time success")
    return data


# merge data (might do sth to pre-process the new data)
def merge_data(cache: dict, new: dict):
    print("merge_data begin")
    for code in cache.keys():
        cache[code].append(new[code])
    print("merge_data success")
    return cache


# algorithm (now just macX, ema calculation)
def do_algorithm(cache: dict, algos: dict):
    print("do_algorithm begin")
    need_report = False
    msg_all = ""

    for code in cache.keys():
        res, msg = algos[code].do(cache[code])
        if res:
            need_report = True
            msg_all += msg + "\n"
    print("do_algorithm success")
    return need_report, msg_all


# report to user use some tools (now we use email to report)
def report(reporter: MSGSender, msg: str):
    msg_arr = msg.split(";")
    for i in range(len(msg_arr) - 1):
        reporter.send(msg_arr[i])


if __name__ == '__main__':
    mysql_handler, msg_reporter, algo_players = init_env()
    result, data_in_cache = prepare_data(mysql_handler, True)
    if result:
        main_loop(data_in_cache, msg_reporter, algo_players, mysql_handler)

    print("done.")



