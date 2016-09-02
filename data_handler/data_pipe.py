from Athena.settings import AthenaConfig
from Athena.data_handler.redis_wrapper import RedisWrapper
HTf, HKf = AthenaConfig.HermesTickFields, AthenaConfig.HermesKLineFields

__author__ = 'zed'


class HermesPipe(object):
    """
    HermesPipe class acts as a pipeline between local redis server
    and remote redis server (deployed on the PC to store historical data)
    It simply pipes hermes data to that PC.
    """
    def __init__(self):
        """

        :return:
        """
        # open connections to local and remote redis
        self.local_redis = RedisWrapper(
            host_name=AthenaConfig.redis_host_local,
            port=AthenaConfig.redis_port,
            db=AthenaConfig.hermes_db_index
        )

        self.remote_redis = RedisWrapper(
            host_name=AthenaConfig.redis_host_remote_1,
            port=AthenaConfig.redis_port,
            db=AthenaConfig.hermes_db_index
        )

        # create a listener
        self.sub = self.remote_redis.connection.pubsub()
        self.subscribed_instruments = []

    def add_instrument(self, instrument, kline_dur_specifiers):
        """
        Begin to listen to one single instrument.
        :param instrument: string
        :param kline_dur_specifiers: tuple of strings.
            Default is ('1m'), subscribe 1 minute kline only.
        :return:
        """
        # if the instrument already subscribed
        if instrument in self.subscribed_instruments:
            print('[Data Pipe]: Already piping {}.'.format(
                instrument))
            return
        # if instrument is not distinguishable:
        if instrument not in AthenaConfig.HermesInstrumentsList.all:
            print('[Data Pipe]: {} is not in Hermes code book.'.format(
                instrument))
            return

        # otherwise
        channels = [AthenaConfig.hermes_md_mapping[instrument]]
        for dur in kline_dur_specifiers:
            channels.append(AthenaConfig.hermes_kl_mapping[dur][instrument])

        # subscribe to channels
        self.sub.subscribe(channels)

        # add instrument to subscribed list
        self.subscribed_instruments.append(instrument)

    def start_pipeline(self):
        """

        :return:
        """
        for message in self.sub.listen():
            if message['type'] == 'message':
                try:
                    # parse message
                    str_message = message['data'].decode('utf-8')
                    list_message = str_message.split('|')

                    # extract key
                    athena_unique_key = list_message[1]

                    # make dictionary data
                    dict_data = dict(
                        zip(list_message[0::2], list_message[1::2])
                    )

                    # publish dict data to local redis.
                    self.local_redis.set_dict(
                        key=athena_unique_key,
                        data=dict_data
                    )

                    # publish str data
                    pub_channel = athena_unique_key.split(':')[0]
                    self.local_redis.connection.publish(
                        channel=pub_channel,
                        message=str_message
                    )
                except UnicodeError:
                    print('[Data Handler]: Unicode error at {}'.format(
                        message
                    ))