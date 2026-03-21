#!/usr/bin/env python3
"""
REST 폴링 기반 모의투자 자동매매 MVP

요구 흐름:
1) 잔고/보유 조회
2) 시세 조회
3) 이동평균 교차 신호 판단
4) 시장가 주문
5) 주문/잔고로 체결 확인
6) 반복

주의:
- WebSocket을 사용하지 않습니다.
- 모의투자 계좌에서만 먼저 테스트하세요.
"""

import argparse
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# 프로젝트 루트 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from kis_backtest import LeanClient
from kis_backtest.models import OrderStatus
from kis_backtest.providers.kis import KISAuth, KISBrokerageProvider, KISDataProvider

KST = ZoneInfo("Asia/Seoul")


@dataclass
class Config:
    symbol: str
    qty: int
    poll_sec: float
    short_window: int
    long_window: int
    max_orders: int
    min_order_interval_sec: int
    order_check_retries: int
    order_check_interval_sec: float


class RestPollingTrader:
    def __init__(self, live, config: Config):
        self.live = live
        self.config = config
        self.running = True
        self.prices = deque(maxlen=config.long_window)
        self.last_ma_relation = None
        self.last_order_ts = 0.0
        self.order_count = 0

    def stop(self, *_):
        self.running = False

    def is_market_open(self, now_kst: datetime) -> bool:
        if now_kst.weekday() >= 5:  # 토/일
            return False

        hhmm = now_kst.hour * 100 + now_kst.minute
        return 900 <= hhmm < 1530

    def get_position_qty(self) -> int:
        positions = self.live.get_positions()
        for pos in positions:
            if pos.symbol == self.config.symbol:
                return int(pos.quantity)
        return 0

    def get_price(self) -> float:
        quote = self.live.get_quote(self.config.symbol)
        return float(quote.price)

    def compute_signal(self, current_qty: int) -> str:
        if len(self.prices) < self.config.long_window:
            return "HOLD"

        short_ma = sum(list(self.prices)[-self.config.short_window:]) / self.config.short_window
        long_ma = sum(self.prices) / self.config.long_window

        relation = "ABOVE" if short_ma > long_ma else "BELOW"
        signal = "HOLD"

        if self.last_ma_relation is not None and relation != self.last_ma_relation:
            if self.last_ma_relation == "BELOW" and relation == "ABOVE" and current_qty <= 0:
                signal = "BUY"
            elif self.last_ma_relation == "ABOVE" and relation == "BELOW" and current_qty > 0:
                signal = "SELL"

        self.last_ma_relation = relation
        return signal

    def can_place_order(self) -> bool:
        now = time.time()
        if now - self.last_order_ts < self.config.min_order_interval_sec:
            return False
        if self.order_count >= self.config.max_orders:
            return False
        return True

    def wait_order_result(self, order_id: str):
        for _ in range(self.config.order_check_retries):
            try:
                orders = self.live.get_orders()
                target = [o for o in orders if o.id == order_id]
                if not target:
                    time.sleep(self.config.order_check_interval_sec)
                    continue

                order = target[0]
                if order.status in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED):
                    return True, order.status.value
                if order.status in (OrderStatus.REJECTED, OrderStatus.CANCELLED):
                    return False, order.status.value
            except Exception:
                # 조회 실패는 일시 오류로 간주하고 재시도
                pass

            time.sleep(self.config.order_check_interval_sec)

        return False, "timeout"

    def run(self):
        print("=" * 70)
        print("REST 폴링 자동매매 시작")
        print("=" * 70)
        print(f"종목: {self.config.symbol}")
        print(f"주기: {self.config.poll_sec}초")
        print(f"전략: MA({self.config.short_window}, {self.config.long_window}) 교차")
        print(f"주문: 시장가 {self.config.qty}주")
        print("운영시간: KST 정규장 09:00~15:30")
        print("=" * 70)

        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        while self.running:
            now_kst = datetime.now(KST)

            if not self.is_market_open(now_kst):
                print(f"[{now_kst.strftime('%H:%M:%S')}] 장외 시간 - 대기")
                time.sleep(self.config.poll_sec)
                continue

            try:
                balance = self.live.get_balance()
                current_qty = self.get_position_qty()
                price = self.get_price()

                self.prices.append(price)
                signal_text = self.compute_signal(current_qty)

                print(
                    f"[{now_kst.strftime('%H:%M:%S')}] "
                    f"price={price:,.0f} qty={current_qty} "
                    f"cash={balance.available_cash:,.0f} signal={signal_text}"
                )

                if signal_text in ("BUY", "SELL") and self.can_place_order():
                    order = self.live.submit_order(
                        symbol=self.config.symbol,
                        side="buy" if signal_text == "BUY" else "sell",
                        quantity=self.config.qty,
                        order_type="market",
                    )
                    self.last_order_ts = time.time()
                    self.order_count += 1

                    print(f"  주문 제출: id={order.id} side={signal_text} qty={self.config.qty}")

                    ok, state = self.wait_order_result(order.id)
                    balance_after = self.live.get_balance()
                    qty_after = self.get_position_qty()

                    print(
                        f"  체결확인: status={state} success={ok} "
                        f"qty_after={qty_after} cash_after={balance_after.available_cash:,.0f}"
                    )

                time.sleep(self.config.poll_sec)
            except Exception as e:
                print(f"[{now_kst.strftime('%H:%M:%S')}] 오류: {e}")
                time.sleep(max(1.0, self.config.poll_sec))

        print("\n자동매매 종료")
        try:
            final_balance = self.live.get_balance()
            final_qty = self.get_position_qty()
            print(
                f"최종 상태: qty={final_qty} "
                f"cash={final_balance.available_cash:,.0f} total={final_balance.total_equity:,.0f}"
            )
        except Exception as e:
            print(f"최종 잔고 조회 실패: {e}")


def create_live_client(mode: str):
    auth = KISAuth.from_env(mode=mode)
    data_provider = KISDataProvider(auth)
    brokerage_provider = KISBrokerageProvider.from_auth(auth)
    client = LeanClient(data_provider=data_provider, brokerage_provider=brokerage_provider)
    return client.live()


def parse_args():
    parser = argparse.ArgumentParser(description="REST 폴링 자동매매 MVP")
    parser.add_argument("--symbol", default="005930", help="종목코드")
    parser.add_argument("--qty", type=int, default=1, help="주문 수량")
    parser.add_argument("--poll-sec", type=float, default=5.0, help="시세 폴링 주기(초)")
    parser.add_argument("--short-window", type=int, default=5, help="단기 이동평균 길이")
    parser.add_argument("--long-window", type=int, default=20, help="장기 이동평균 길이")
    parser.add_argument("--max-orders", type=int, default=20, help="세션 최대 주문 건수")
    parser.add_argument("--min-order-interval-sec", type=int, default=15, help="최소 주문 간격(초)")
    parser.add_argument("--order-check-retries", type=int, default=8, help="주문 상태 확인 재시도 횟수")
    parser.add_argument("--order-check-interval-sec", type=float, default=2.0, help="주문 상태 확인 간격(초)")
    parser.add_argument(
        "--mode",
        choices=["paper", "live"],
        default="paper",
        help="인증 모드 (기본: paper)",
    )
    return parser.parse_args()


def validate_args(args):
    if args.short_window <= 0 or args.long_window <= 0:
        raise ValueError("이동평균 길이는 1 이상이어야 합니다.")
    if args.short_window >= args.long_window:
        raise ValueError("short-window는 long-window보다 작아야 합니다.")
    if args.qty <= 0:
        raise ValueError("qty는 1 이상이어야 합니다.")


if __name__ == "__main__":
    args = parse_args()
    validate_args(args)

    live = create_live_client(mode=args.mode)
    cfg = Config(
        symbol=args.symbol,
        qty=args.qty,
        poll_sec=args.poll_sec,
        short_window=args.short_window,
        long_window=args.long_window,
        max_orders=args.max_orders,
        min_order_interval_sec=args.min_order_interval_sec,
        order_check_retries=args.order_check_retries,
        order_check_interval_sec=args.order_check_interval_sec,
    )

    trader = RestPollingTrader(live, cfg)
    trader.run()
