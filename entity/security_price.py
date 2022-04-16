class SecurityPrice:
    def __init__(self, time, code, open, close, low, high, volume, money, paused):
        self.time_ = time
        self.code_ = code
        self.open_ = open
        self.close_ = close
        self.low_ = low
        self.high_ = high
        self.volume_ = volume
        self.money_ = money
        self.paused_ = paused
        self.sql_data = "(\"" + self.code_ + "\", \"" + str(self.time_) + "\", " + str(self.open_) + ", " \
                        + str(self.close_) + ", " + str(self.high_) \
                        + ", " + str(self.low_) + ", " + str(self.volume_) + ", " + str(self.money_) + ", " \
                        + str(self.paused_) + ")"
        self.table_scheme = 'security_price(code, time, open, close, low, high, volume, money, paused)'

    def gen_insert_sql(self):
        return "insert into " + self.table_scheme + " values" + self.sql_data + ";"


def make_from_data_frame(code, df) -> SecurityPrice:
    return SecurityPrice(df.name, code, df.open, df.close, df.high, df.low, df.volume, df.money, df.paused)


def make_from_sql(t) -> SecurityPrice:
    return SecurityPrice(t[2], t[1], t[3], t[4], t[5], t[6], t[7], t[8], t[9])


def gen_query_sql(code, time):
    return "select * from security_price where code = \"" + str(code) + "\" and time = \"" + str(time) + "\";"
