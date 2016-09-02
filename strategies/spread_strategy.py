from Athena.settings import AthenaConfig
from Athena.containers import OrderEvent
from Athena.strategies.strategy import StrategyTemplate

Kf = AthenaConfig.HermesKLineFields

__author__ = 'zed'


class SpreadStrategy1(StrategyTemplate):
    """

    """
    strategy_name_prefix = 'strategy:spread_1'
    param_names = ['band_width', 'band_mean', 'stop_win']
    net_position_limit = 20

    def __init__(self, subscribe_list, param_list,
                 pair, train=False):
        """

        :param subscribe_list:
        :param param_list:
        :param pair
        """
        super(SpreadStrategy1, self).__init__(subscribe_list)
        self._map_to_channels(param_list,
                              suffix='.'.join(pair), full_name=train)

        # spot and future leg names
        self.spot_leg = pair[0]
        self.future_leg = pair[1]

        # parameters
        self.band_width = self.param_dict['band_width']
        self.band_up = self.param_dict['band_mean'] + self.band_width
        self.band_down = self.param_dict['band_mean'] - self.band_width

        self.net_positions = 0

    def __transact_spread(self, direction, message, is_stop_win=False):
        """

        :param direction:
        :param message:
        :param is_stop_win:
        :return:
        """
        #
        if (
            direction == 'long'
        ) and (
            abs(self.net_positions) < SpreadStrategy1.net_position_limit
        ):
            # cover or open
            if self.net_positions >= 0:
                this_type = 'open_long'
                commission = 0.05
            else:
                this_type = 'cover_short'
                commission = 0
            this_subtype = None

            if is_stop_win:
                # abandon this stopwin
                # if net positions are already covered
                this_subtype = 'stop_win'
                if self.net_positions >= 0: return

            # long spread, long future short spot
            order_future = OrderEvent(
                direction='long',
                type=this_type,
                subtype=this_subtype,
                quantity=1,
                contract=self.future_leg,
                price=message[self.future_leg],
                update_time=message[Kf.close_time],
                commission=commission,
                bar_count=message[Kf.count]
            )

            order_spot = OrderEvent(
                direction='short',
                type=this_type,
                subtype=this_subtype,
                quantity=1,
                contract=self.spot_leg,
                price=message[self.spot_leg],
                update_time=message[Kf.close_time],
                commission=commission,
                bar_count=message[Kf.count]
            )

            self.publish(order_future, plot=True)
            self.publish(order_spot, plot=True)

            # increment to opening positions
            self.net_positions += 1

        elif (
            direction == 'short'
        ) and (
            abs(self.net_positions) < SpreadStrategy1.net_position_limit
        ):
            # cover or open
            if self.net_positions <= 0:
                this_type = 'open_short'
                commission = 0.05
            else:
                this_type = 'cover_long'
                commission = 0
            this_subtype = None

            if is_stop_win:
                this_subtype = 'stop_win'
                # abandon this stopwin
                # if net positions are already covered
                if self.net_positions <= 0: return

            # short spread, long spot short future
            order_future = OrderEvent(
                direction='short',
                type=this_type,
                subtype=this_subtype,
                quantity=1,
                contract=self.future_leg,
                price=message[self.future_leg],
                update_time=message[Kf.close_time],
                commission=commission,
                bar_count=message[Kf.count]
            )

            order_spot = OrderEvent(
                direction='long',
                type=this_type,
                subtype=this_subtype,
                quantity=1,
                contract=self.spot_leg,
                price=message[self.spot_leg],
                update_time=message[Kf.close_time],
                commission=commission,
                bar_count=message[Kf.count]
            )

            self.publish(order_future, plot=True)
            self.publish(order_spot, plot=True)

            # increment to opening positions
            self.net_positions -= 1

        else: return

    def __stopwin_logic(self, message):
        """

        :param message:
        :return:
        """
        quantity = int(message['quantity'])
        for i in range(max(quantity, int(self.net_positions))):
            self.__transact_spread(message['direction'], message, True)

    def on_message(self, message):
        """

        :param message:
        :return:
        """
        if message['tag'] == 'spread':

            # update the band
            self.band_down = message['band_mean'] - self.band_width
            self.band_up = message['band_mean'] + self.band_width

            # on spread signal event
            if message['spread_sell'] > self.band_up:
                self.__transact_spread('short', message)
            elif message['spread_buy'] < self.band_down:
                self.__transact_spread('long', message)

        elif message['tag'] == 'stop':

            # on stopwin signals
            if message['direction'] in ['long', 'short']:
                self.__stopwin_logic(message)

