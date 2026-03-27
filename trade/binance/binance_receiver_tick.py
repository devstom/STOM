
import re
import sys
import time
import sqlite3
import binance
import numpy as np
import pandas as pd
from traceback import format_exc
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QThread, pyqtSignal, QTimer
from trade.binance.binance_websocket import WebSocketReceiver
from utility.setting_base import ui_num, DB_COIN_TICK, DB_COIN_MIN
from utility.static import now, timedelta_sec, str_ymdhms_utc, now_utc, str_hms, str_ymd


class Updater(QThread):
    signal1 = pyqtSignal(tuple)
    signal2 = pyqtSignal()

    def __init__(self, creceivQ):
        super().__init__()
        self.creceivQ = creceivQ

    def run(self):
        while True:
            data = self.creceivQ.get()
            if data.__class__ == tuple:
                self.signal1.emit(data)
            elif data.__class__ == str:
                self.signal2.emit()


class BinanceReceiverTick:
    def __init__(self, qlist, dict_set):
        """
        windowQ, soundQ, queryQ, teleQ, chartQ, hogaQ, webcQ, backQ, creceivQ, ctraderQ,  cstgQ, liveQ, wdzservQ
           0        1       2      3       4      5      6      7       8         9         10     11      12
        """
        app = QApplication(sys.argv)

        self.windowQ     = qlist[0]
        self.soundQ      = qlist[1]
        self.teleQ       = qlist[3]
        self.hogaQ       = qlist[5]
        self.creceivQ    = qlist[8]
        self.ctraderQ    = qlist[9]
        self.cstgQ       = qlist[10]
        self.dict_set    = dict_set

        self.dict_dtdm   = {}
        self.dict_jgdt   = {}
        self.dict_data   = {}
        self.dict_daym   = {}
        self.dict_mtop   = {}
        self.dict_prec   = {}
        self.dict_dlhp   = {}
        self.dict_money  = {}
        self.dict_bmbyp  = {}
        self.dict_smbyp  = {}
        self.dict_index  = {}

        self.list_hgdt   = [0, 0, 0, 0]
        self.list_gsjm   = []
        self.tuple_jango = ()
        self.tuple_order = ()

        self.int_logt    = 0
        self.int_mtdt    = None
        self.hoga_code   = None
        self.chart_code  = None
        self.codes       = None
        self.last_gsjm   = None
        self.binance     = binance.Client()

        self.dict_bool = {
            '프로세스종료': False
        }

        self.lvhp_time = now()

        self.GetTickers()

        self.ws_thread = WebSocketReceiver(self.codes, self.windowQ)
        self.ws_thread.signal1.connect(self.UpdateTickData)
        self.ws_thread.signal2.connect(self.UpdateHogaData)
        self.ws_thread.start()

        self.qtimer = QTimer()
        self.qtimer.setInterval(1 * 1000)
        self.qtimer.timeout.connect(self.Scheduler)
        self.qtimer.start()

        self.updater = Updater(self.creceivQ)
        self.updater.signal1.connect(self.UpdateTuple)
        self.updater.signal2.connect(self.SysExit)
        self.updater.start()

        app.exec_()

    def GetTickers(self):
        dict_daym = {}
        try:
            datas = self.binance.futures_ticker()
        except:
            self.windowQ.put((ui_num['시스템로그'], f'{format_exc()}오류 알림 - self.binance.futures_ticker()'))
        else:
            datas = [data for data in datas if re.search('USDT$', data['symbol']) is not None]
            ymd   = str_ymd(now_utc())
            for data in datas:
                code = data['symbol']
                if code not in self.dict_data:
                    c    = float(data['lastPrice'])
                    o    = float(data['openPrice'])
                    h    = float(data['highPrice'])
                    low  = float(data['lowPrice'])
                    per  = round(float(data['priceChangePercent']), 2)
                    dm   = float(data['quoteVolume'])
                    prec = round(c - float(data['priceChange']), 8)
                    self.dict_data[code] = [c, o, h, low, per, dm, 0, 0, 0, 0, 0, c, c, c]
                    self.dict_prec[code] = [ymd, prec]
                    dict_daym[code] = dm

        self.codes = list(self.dict_data)
        self.list_gsjm = [x for x, y in sorted(dict_daym.items(), key=lambda x: x[1], reverse=True)[:10]]
        data = tuple(self.list_gsjm)
        self.cstgQ.put(('관심목록', data))

        text = '코인 리시버를 시작하였습니다.'
        if self.dict_set['코인알림소리']: self.soundQ.put(text)
        self.teleQ.put(text)
        self.windowQ.put((ui_num['기본로그'], '시스템 명령 실행 알림 - 리시버 시작'))

    def EnsureMoneyState(self, code, c, buy_money=0.0, sell_money=0.0):
        if code in self.dict_money:
            return

        # 초당/분당매수금액, 초당/분당매도금액, 당일매수금액, 최고매수금액, 최고매수가격, 당일매도금액, 최고매도금액, 최고매도가격
        #     0          1          2          3          4          5          6          7
        self.dict_money[code] = [buy_money, sell_money, buy_money, buy_money, c, sell_money, sell_money, c]
        self.dict_index[code] = {c: 0}
        self.dict_bmbyp[code] = np.zeros(1000, dtype=np.float64)
        self.dict_smbyp[code] = np.zeros(1000, dtype=np.float64)
        self.dict_bmbyp[code][0] = buy_money
        self.dict_smbyp[code][0] = sell_money
        self.dict_index[code]['count'] = 1

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

        code_data = self.dict_data[code]
        ymd = str(dt)[:8]
        if ymd != self.dict_prec[code][0]:
            self.dict_prec[code] = [ymd, code_data[0]]
            bids, asks, pretbids, pretasks = 0, 0, 0, 0
            o, h, low = c, c, c
            dm = int(v * c)
        else:
            dm, _, bids, asks, pretbids, pretasks = code_data[5:11]
            o, h, low = code_data[1:4]
            if c > h: h = c
            if c < low: low = c
            dm = int(dm + v * c)

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
        self.dict_data[code] = [c, o, h, low, per, dm, ch, bids, asks, tbids, tasks]

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
            dt = int(str_ymdhms_utc(data['T']))
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
                if dt > code_dtdm[0]:
                    send = True
            else:
                self.dict_dtdm[code] = [dt, 0]
                code_dtdm = self.dict_dtdm[code]
                send = True

            if send:
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

                data = [dt] + code_data[:9] + [tm, hlp, lhp] + money_arr + \
                    hoga_seprice + hoga_buprice + hoga_samount + hoga_bamount + hoga_tamount + \
                    [hjt, gsjm, code, logt]

                self.cstgQ.put(data)
                if code in self.tuple_order or code in self.tuple_jango:
                    self.ctraderQ.put(('잔고갱신', (code, c)))

                code_dtdm[0] = dt
                code_dtdm[1] = dm
                code_data[7] = 0
                code_data[8] = 0
                money_arr[0] = 0
                money_arr[1] = 0

                self.dict_dlhp[code] = lhp

                if logt != 0:
                    gap = (now() - receivetime).total_seconds()
                    self.windowQ.put((ui_num['타임로그'], f'리시버 연산 시간 알림 - 수신시간과 연산시간의 차이는 [{gap:.6f}]초입니다.'))
                    self.int_logt = dt_min

        if self.int_mtdt is None:
            self.int_mtdt = dt
        elif self.int_mtdt < dt:
            self.dict_mtop[self.int_mtdt] = ';'.join(self.list_gsjm)
            self.int_mtdt = dt

        if self.hoga_code == code and dt > self.list_hgdt[1]:
            self.list_hgdt[1] = dt
            self.hogaQ.put([code] + hoga_tamount + hoga_seprice[-5:] + hoga_buprice[:5] + hoga_samount[-5:] + hoga_bamount[:5])

    def Scheduler(self):
        self.MoneyTopSearch()

        curr_time = now()
        if not self.dict_set['바이낸스선물고정레버리지'] and curr_time > self.lvhp_time:
            if self.dict_dlhp:
                self.ctraderQ.put(('저가대비고가등락율', self.dict_dlhp))
            self.lvhp_time = timedelta_sec(300)

        inthmsutc = int(str_hms(now_utc()))
        if not self.dict_bool['프로세스종료'] and \
                ((self.dict_set['코인전략종료시간'] < inthmsutc < self.dict_set['코인전략종료시간'] + 10 and self.dict_set['코인프로세스종료']) or 235000 < inthmsutc < 235010):
            self.ReceiverProcKill()

        current_gsjm = tuple(self.list_gsjm)
        if current_gsjm != self.last_gsjm:
            self.cstgQ.put(('관심목록', current_gsjm))
            self.last_gsjm = current_gsjm

    def MoneyTopSearch(self):
        if self.dict_daym:
            list_mtop = [x for x, y in sorted(self.dict_daym.items(), key=lambda x: x[1], reverse=True)[:10]]
            insert_set = set(list_mtop) - set(self.list_gsjm)
            delete_set = set(self.list_gsjm) - set(list_mtop)
            if insert_set:
                for code in insert_set:
                    self.InsertGsjmlist(code)
            if delete_set:
                for code in delete_set:
                    self.DeleteGsjmlist(code)

    def InsertGsjmlist(self, code):
        if code not in self.list_gsjm:
            self.list_gsjm.append(code)
            if self.dict_set['코인매도취소관심진입']:
                self.ctraderQ.put(('관심진입', code))

    def DeleteGsjmlist(self, code):
        if code in self.list_gsjm:
            self.list_gsjm.remove(code)
            if self.dict_set['코인매수취소관심이탈']:
                self.ctraderQ.put(('관심이탈', code))

    def ReceiverProcKill(self):
        self.dict_bool['프로세스종료'] = True
        self.WebProcessKill()
        if self.dict_set['코인알림소리']:
            self.soundQ.put('바이낸스 시스템을 3분 후 종료합니다.')
        QTimer.singleShot(180 * 1000, lambda: self.creceivQ.put('프로세스종료'))

    def WebProcessKill(self):
        if self.ws_thread:
            self.ws_thread.stop()
            self.ws_thread.terminate()

    def UpdateTuple(self, data):
        gubun, data = data
        if gubun == '잔고목록':
            self.tuple_jango = data
        elif gubun == '주문목록':
            self.tuple_order = data
        elif gubun == '호가종목코드':
            self.hoga_code = data
        elif gubun == '차트종목코드':
            self.chart_code = data
        elif gubun == '수동데이터저장':
            self.ReceiverProcKill()
        elif gubun == '설정변경':
            self.dict_set = data
            if not self.dict_set['코인리시버'] and not self.dict_set['코인트레이더']:
                self.creceivQ.put('프로세스종료')

    def SysExit(self):
        if self.dict_set['코인데이터저장']:
            self.SaveData()
        else:
            self.cstgQ.put('프로세스종료')
        self.ctraderQ.put('프로세스종료')
        time.sleep(5)
        self.windowQ.put((ui_num['기본로그'], '시스템 명령 실행 알림 - 리시버 종료'))

    def SaveData(self):
        codes = set()
        if self.dict_mtop:
            for mtop_text in self.dict_mtop.values():
                codes.update(mtop_text.split(';'))
            con = sqlite3.connect(DB_COIN_TICK if self.dict_set['코인타임프레임'] else DB_COIN_MIN)
            last_index = 0
            try:
                df = pd.read_sql(f'SELECT * FROM moneytop ORDER BY "index" DESC LIMIT 1', con)
                last_index = df['index'][0]
            except:
                pass
            dict_mtop = {key: value for key, value in self.dict_mtop.items() if key > last_index}
            df = pd.DataFrame(dict_mtop.values(), columns=['거래대금순위'], index=list(dict_mtop))
            df.to_sql('moneytop', con, if_exists='append', chunksize=1000)
            con.close()
            self.windowQ.put((ui_num['기본로그'], '시스템 명령 실행 알림 - 거래대금순위 저장 완료'))

        self.cstgQ.put(('데이터저장', codes))
