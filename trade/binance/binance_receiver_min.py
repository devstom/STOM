
import numpy as np
from traceback import format_exc
from utility.setting_base import ui_num
from utility.static import now, str_ymdhms_utc
from trade.binance.binance_receiver_tick import BinanceReceiverTick


class BinanceReceiverMin(BinanceReceiverTick):
    def UpdateTickData(self, data):
        try:
            data = data['data']
            dt   = int(str_ymdhms_utc(data['T']))
            code = data['s']
            c    = float(data['p'])
            v    = float(data['q'])
            m    = data['m']
        except:
            self.windowQ.put((ui_num['시스템로그'], f'{format_exc()}오류 알림 - UpdateTickData'))
            return

        if code in self.tuple_jango and (code not in self.dict_jgdt or dt > self.dict_jgdt[code]):
            self.ctraderQ.put(('잔고갱신', (code, c)))
            self.dict_jgdt[code] = dt

        code_data = self.dict_data[code]
        ymd = str(dt)[:8]
        if ymd != self.dict_prec[code][0]:
            self.dict_prec[code] = [ymd, code_data[0]]
            bids, asks, pretbids, pretasks = 0, 0, 0, 0
            o, h, low = c, c, c
            dm = round(v * c, 2)
            mo = mh = ml = c
        else:
            dm, _, bids, asks, pretbids, pretasks = code_data[5:11]
            o, h, low = code_data[1:4]
            if c > h: h = c
            if c < low: low = c
            dm = round(dm + v * c, 2)

            if bids == 0 and asks == 0:
                mo = mh = ml = c
            else:
                mo, mh, ml = code_data[-3:]
                if mh < c: mh = c
                if ml > c: ml = c

        bids_ = v if not m else 0
        asks_ = 0 if not m else v
        bids += bids_
        asks += asks_
        tbids = round(pretbids + bids_, 8)
        tasks = round(pretasks + asks_, 8)
        # noinspection PyTypeChecker
        ch = min(500, round(tbids / tasks * 100, 2)) if tasks > 0 else 500
        per = round((c / self.dict_prec[code][1] - 1) * 100, 2)

        self.dict_daym[code] = dm
        self.dict_data[code] = [c, o, h, low, per, dm, ch, bids, asks, tbids, tasks, mo, mh, ml]

        buy_money = c * bids_
        sell_money = c * asks_
        if code not in self.dict_money:
            self.EnsureMoneyState(code, c, buy_money, sell_money)
        else:
            money_arr = self.dict_money[code]
            price_idx = self.dict_index[code]
            buy_arr = self.dict_bmbyp[code]
            sell_arr = self.dict_smbyp[code]

            money_arr[0] += buy_money
            money_arr[1] += sell_money
            money_arr[2] += buy_money
            money_arr[5] += sell_money

            idx = price_idx.get(c)
            if idx is not None:
                buy_arr[idx] += buy_money
                sell_arr[idx] += sell_money
            else:
                idx = price_idx['count']
                if idx >= len(buy_arr):
                    self.dict_bmbyp[code] = np.resize(buy_arr, len(buy_arr) * 2)
                    self.dict_smbyp[code] = np.resize(sell_arr, len(sell_arr) * 2)
                    buy_arr = self.dict_bmbyp[code]
                    sell_arr = self.dict_smbyp[code]

                price_idx[c] = idx
                buy_arr[idx] = buy_money
                sell_arr[idx] = sell_money
                price_idx['count'] += 1

            if buy_arr[idx] >= money_arr[3]:
                money_arr[3] = buy_arr[idx]
                money_arr[4] = c
            if sell_arr[idx] >= money_arr[6]:
                money_arr[6] = sell_arr[idx]
                money_arr[7] = c

        dt_ = int(str(dt)[:13])
        data_dlhp = self.dict_dlhp.get(code)
        if data_dlhp:
            if dt_ != data_dlhp[0]:
                data_dlhp[0] = dt_
                data_dlhp[1] = round((h / low - 1) * 100, 2)
        else:
            self.dict_dlhp[code] = [dt_, round((h / low - 1) * 100, 2)]

        if self.hoga_code == code:
            bids, asks = self.list_hgdt[2:4]
            if bids_ > 0: bids += bids_
            if asks_ > 0: asks += asks_
            self.list_hgdt[2:4] = bids, asks
            if dt > self.list_hgdt[0]:
                self.hogaQ.put((code, c, per, 0, -1, o, h, low))
                if asks > 0: self.hogaQ.put((-asks, ch))
                if bids > 0: self.hogaQ.put((bids, ch))
                self.list_hgdt[0] = dt
                self.list_hgdt[2:4] = [0, 0]

    def UpdateHogaData(self, data):
        try:
            data = data['data']
            dt   = int(str_ymdhms_utc(data['T']))
            if self.dict_set['코인전략종료시간'] < int(str(dt)[8:]):
                return

            code = data['s']
            hoga_seprice = [
                float(data['a'][9][0]), float(data['a'][8][0]), float(data['a'][7][0]), float(data['a'][6][0]), float(data['a'][5][0]),
                float(data['a'][4][0]), float(data['a'][3][0]), float(data['a'][2][0]), float(data['a'][1][0]), float(data['a'][0][0])
            ]
            hoga_buprice = [
                float(data['b'][0][0]), float(data['b'][1][0]), float(data['b'][2][0]), float(data['b'][3][0]), float(data['b'][4][0]),
                float(data['b'][5][0]), float(data['b'][6][0]), float(data['b'][7][0]), float(data['b'][8][0]), float(data['b'][9][0])
            ]
            hoga_samount = [
                float(data['a'][9][1]), float(data['a'][8][1]), float(data['a'][7][1]), float(data['a'][6][1]), float(data['a'][5][1]),
                float(data['a'][4][1]), float(data['a'][3][1]), float(data['a'][2][1]), float(data['a'][1][1]), float(data['a'][0][1])
            ]
            hoga_bamount = [
                float(data['b'][0][1]), float(data['b'][1][1]), float(data['b'][2][1]), float(data['b'][3][1]), float(data['b'][4][1]),
                float(data['b'][5][1]), float(data['b'][6][1]), float(data['b'][7][1]), float(data['b'][8][1]), float(data['b'][9][1])
            ]
            hoga_tamount = [
                round(sum(hoga_samount), 8), round(sum(hoga_bamount), 8)
            ]
            receivetime = now()
        except:
            self.windowQ.put((ui_num['시스템로그'], f'{format_exc()}오류 알림 - UpdateHogaData'))
            return

        send   = False
        dt_min = int(str(dt)[:12])

        code_data = self.dict_data.get(code)
        if code_data:
            code_dtdm = self.dict_dtdm.get(code)
            if code_dtdm:
                if dt_min > code_dtdm[0]:
                    send = True
            else:
                self.dict_dtdm[code] = [dt_min, 0]
                code_dtdm = self.dict_dtdm[code]

            if send or code == self.chart_code or code in self.list_gsjm:
                c, _, h, low, _, dm, _, bids, asks = code_data[:9]
                csp = cbp = c

                if hoga_seprice[-1] < csp:
                    valid_indices = [i for i, price in enumerate(hoga_seprice) if price >= csp]
                    end_idx = valid_indices[-1] + 1 if valid_indices else None
                    if end_idx is not None:
                        start_idx = max(end_idx - 5, 0)
                        add_cnt   = max(5 - end_idx, 0)
                        hoga_seprice = [0.] * add_cnt + hoga_seprice[start_idx:end_idx]
                        hoga_samount = [0.] * add_cnt + hoga_samount[start_idx:end_idx]
                    else:
                        hoga_seprice = [0.] * 5
                        hoga_samount = [0.] * 5
                else:
                    hoga_seprice = hoga_seprice[-5:]
                    hoga_samount = hoga_samount[-5:]

                if hoga_buprice[0] > cbp:
                    valid_indices = [i for i, price in enumerate(hoga_buprice) if price <= cbp]
                    start_idx = valid_indices[0] if valid_indices else None
                    if start_idx is not None:
                        end_idx   = min(start_idx + 5, 10)
                        add_cnt   = max(start_idx - 5, 0)
                        hoga_buprice = hoga_buprice[start_idx:end_idx] + [0.] * add_cnt
                        hoga_bamount = hoga_bamount[start_idx:end_idx] + [0.] * add_cnt
                    else:
                        hoga_buprice = [0.] * 5
                        hoga_bamount = [0.] * 5
                else:
                    hoga_buprice = hoga_buprice[:5]
                    hoga_bamount = hoga_bamount[:5]

                self.EnsureMoneyState(code, c)
                money_arr = self.dict_money[code]

                tm = dm - code_dtdm[1]
                if tm == dm and 500 < int(str(dt)[8:]): tm = 0
                hlp  = round((c / ((h + low) / 2) - 1) * 100, 2)
                lhp  = round((h / low - 1) * 100, 2)
                hjt  = sum(hoga_samount + hoga_bamount)
                gsjm = 1 if code in self.list_gsjm else 0
                logt = now() if self.int_logt < dt_min else 0
                dt_  = code_dtdm[0]

                data = [dt_] + code_data[:9] + code_data[11:] + [tm, hlp, lhp] + money_arr + \
                    hoga_seprice + hoga_buprice + hoga_samount + hoga_bamount + hoga_tamount + \
                    [hjt, gsjm, code, logt, send]

                self.cstgQ.put(data)
                if send:
                    if code in self.tuple_jango:
                        self.ctraderQ.put(('주문확인', (code, c)))

                    code_dtdm[0] = dt_min
                    code_dtdm[1] = dm
                    code_data[7] = 0
                    code_data[8] = 0
                    money_arr[0] = 0
                    money_arr[1] = 0

                if logt != 0:
                    gap = (now() - receivetime).total_seconds()
                    self.windowQ.put((ui_num['타임로그'], f'리시버 연산 시간 알림 - 수신시간과 연산시간의 차이는 [{gap:.6f}]초입니다.'))
                    self.int_logt = dt_min

        if self.int_mtdt is None:
            self.int_mtdt = dt_min
        elif self.int_mtdt < dt_min:
            self.dict_mtop[self.int_mtdt] = ';'.join(self.list_gsjm)
            self.int_mtdt = dt_min

        if self.hoga_code == code and dt > self.list_hgdt[1]:
            self.list_hgdt[1] = dt
            self.hogaQ.put([code] + hoga_tamount + hoga_seprice[-5:] + hoga_buprice[:5] + hoga_samount[-5:] + hoga_bamount[:5])
