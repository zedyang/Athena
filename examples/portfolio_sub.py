from Athena.portfolio.portfolio import Portfolio

__author__ = 'zed'


def portfolio_sub():
    my_portfolio = Portfolio(
        instruments_list=['GC1608'],
        init_cash=10000,
        strategy_channels=['strategy:cta_1']
    )
    my_portfolio.start()

if __name__ == '__main__':
    portfolio_sub()