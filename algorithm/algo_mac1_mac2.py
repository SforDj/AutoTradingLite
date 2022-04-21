from datetime import datetime, timedelta
import time

from common.config import g_map


class AlgoMac1Mac2:
    def __init__(self, code: str):
        self.code = code
        self.xa25 = []
        self.mac1 = []
        self.mac2 = []

        self.last_mac1 = 0
        self.last_mac2 = 0

        self.DQ = 9
        self.CQ = 16

        self.last_report_time = datetime.fromtimestamp(time.time())
        self.last_report_up = False

    def do(self, sp_list: list):
        result = False
        msg = ""

        curr_time = sp_list[-1].time_

        sp_price_list = [sp.close_ for sp in sp_list]
        xa25_item = (ema(sp_price_list, self.DQ) - ema(sp_price_list, self.CQ)) * 100.0

        print("ema DQ = " + str(ema(sp_price_list, self.DQ)))
        print("ema CQ = " + str(ema(sp_price_list, self.CQ)))

        self.xa25.append(xa25_item)

        mac1_item = ema(self.xa25, 1)
        self.mac1.append(mac1_item)

        if len(self.mac1) < 5:
            if on_the_hour(curr_time) :
                if len(sp_list) > 20:
                    del sp_list[0]
                self.last_mac1 = mac1_item
            else:
                sp_list.pop()
            return False, "no enough data [sizeof mac1 less than 5]"

        mac2_item = ema(self.mac1, 5)
        self.mac2.append(mac2_item)

        if (self.last_mac2 != 0 and self.last_mac1 != 0) and self.last_mac2 >= self.last_mac1 and mac2_item < mac1_item:
            this_report_up = True
            result, msg = True, "股票: " + g_map[self.code] + ": " + "mac1 上穿 mac2 时间：" + str(curr_time) + ";"
        elif (self.last_mac2 != 0 and self.last_mac1 != 0) and self.last_mac2 < self.last_mac1 and mac2_item >= mac1_item:
            this_report_up = False
            result, msg = True, "股票: " + g_map[self.code] + ": " + "mac1 下破 mac2 时间：" + str(curr_time) + ";"
        else:
            this_report_up = self.last_report_up
            result, msg = False, ""

        print("mac1 arr: " + str(self.mac1))
        print("mac2 arr: " + str(self.mac2))
        print(str(result) + ": " + msg)
        need_update = self.post_process_data(sp_list, curr_time)
        if need_update:
            self.last_mac1 = mac1_item
            self.last_mac2 = mac2_item

        if curr_time - self.last_report_time < timedelta(minutes=5) and this_report_up == self.last_report_up:
            result, msg = False, ""

        return result, msg

    def post_process_data(self, sp_list, curr_time):
        if on_the_hour(curr_time):
            if len(self.mac1) > 20 and len(self.mac2) > 20 and len(self.xa25) > 20:
                del sp_list[0], self.mac1[0], self.mac2[0], self.xa25[0]
            return True
        else:
            sp_list.pop()
            self.mac1.pop()
            self.mac2.pop()
            self.xa25.pop()
            return False


def ema(arr: list, N: int):
    if len(arr) == 1:
        return arr[0]

    res = (2 * arr[-1] + (N - 1.0) * ema(arr[:len(arr) - 1], N)) / (N + 1.0)

    return res


def on_the_hour(curr_time: datetime):
    if curr_time.minute == 30 or curr_time.minute == 0:
        return True
    else:
        return False


# if __name__ == '__main__':
#     arr = [200.8, 193.51, 189.78, 191.97, 189.67]
#     print(ema(arr, 5))