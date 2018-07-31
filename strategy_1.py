# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from random import randint
"""
二八轮动+股息率选股策略
二八轮动原则：
    1. A股中规模很大的权重股（流通股大于1亿）占20%；
    2. A股中中小盘股数量级占到80%；
    3. 这20%和80%股的走势存在分化，需要在两者之间不断切换，轮流持有；
    4. 这里用沪深300表示20%的权重股，中证500表示80%的中小盘股；
1、根据二八轮动原则，选择两者当天收盘价较X天前涨幅较大的作为第二天持有的股种，如果都没有涨，则持有国债；
股息率原则：
    1. 选出股息率大于X，营收增长率大于Y的股票；
2、根据股息率原则，在优势股种（沪深300或者中证500）中选择较优质的股票入手；
# TODO 与止损、止盈策略联动
"""
def init(context):
    # 二八轮动相关
    context.hold_type = "CSI300.INDX"
    context.candicate_type = "CSI300.INDX"
    context.time_span = 20 # 20天
    context.hs_stock_list = index_components("沪深300")
    context.zz_stock_list = index_components("中证500")
    context.gz_stock_list = ["000012.XSHG"]
    # context.gz_stock_list = index_components("国债指数")
    # 股息率相关
    context.dividend_yield_min = 4 # 最小股息率%
    context.inc_operating_revenue_min = 4 # 营收增长率%
    context.candicate_num = 4
    # 止损相关
    context.stop_period = 20 # 持股天数
    context.stop_return = 0.04 # 回报率
    context.time = pd.DataFrame()
    context.drawdown = 0.04 # 最大回撤
    context.maxvalue = pd.DataFrame()
    
    # 交易候选股票
    context.candicate_stocks = []
    
    # 更新股票池
    update_universe(context.hs_stock_list+context.zz_stock_list)
    
    # rebalance
    scheduler.run_daily(rebalance)
    scheduler.run_daily(stop)
    
def record(context, stock, record_type='buy'):
    """
    持股记录：
    1、记录股票购买时间；
    2、记录股票最大股价；
    """
    if record_type == 'buy':
        # 记录下单时间及股价最大值
        sotck_time = pd.DataFrame({str(stock): [context.now.replace(tzinfo=None)]})
        maxvalue = pd.DataFrame({str(stock): [context.portfolio.positions[stock].avg_price*context.portfolio.positions[stock].quantity]})
        logger.info('buy stock({}) time is {}, value is {}!'.format(stock, sotck_time, maxvalue))
        context.time = pd.concat([context.time, sotck_time], axis=1, join='inner')
        context.maxvalue = pd.concat([context.maxvalue, maxvalue], axis=1, join='inner')
    if record_type == 'sell':
        # 清除下单时间及股价最大值
        if stock in context.time and stock in context.maxvalue:
            logger.info('sell stock({}) time is {}, value is {}!'.format(stock, context.time[stock], context.maxvalue[stock]))
            del context.time[stock]
            del context.maxvalue[stock]
            
def stop(context, bar_dict):
    """
    止损策略：
    1、在一定的时间（x天）内，受益没有达到指定值（y%），止损；
    2、最大回撤达到指定值（z%），止损；
    """
    # 止损策略1：时间是有价值的
    stop_by_time(context, bar_dict)
    # 止损策略2：最大回撤
    stop_by_drawdown(context, bar_dict)
        
def stop_by_time(context, bar_dict):
    for stock in context.portfolio.positions:
        if stock not in context.time.columns or context.portfolio.positions[stock].quantity == 0:
            logger.error('stock({}) not recorded!'.format(stock))
            continue
        buy_time = context.time[stock][0]
        curr_time = context.now.replace(tzinfo=None)
        
        logger.info('stock({}) buy time is: {}, curr_time is {}!'.format(stock, buy_time, curr_time))
        
        # 持有天数
        position_days = (curr_time - buy_time).days
        # 总体回报率
        toatl_return = (context.portfolio.positions[stock].market_value*context.portfolio.positions[stock].quantity)/(context.portfolio.positions[stock].market_value*context.portfolio.positions[stock].quantity-context.portfolio.positions[stock].pnl)
        
        if position_days > context.stop_period and context.portfolio.positions[stock].pnl < context.stop_return:
            logger.warn('stock({}) position days({}), total return({}) less than ({}), sell it!'.format(stock, position_days, toatl_return, context.stop_return))
            order_obj = order_target_percent(stock, 0)
            if order_obj and order_obj.status != ORDER_STATUS.REJECTED:
                record(context, stock, 'sell')
        logger.info('stock({}) position days({}), toatl return({})!'.format(stock, position_days, toatl_return))
        
def stop_by_drawdown(context, bar_dict):
    for stock in context.portfolio.positions:
        if stock not in context.maxvalue.columns:
            logger.error('stock({}) not recorded!'.format(stock))
            continue
        
        max_value = context.maxvalue[stock][0]
        curr_value = context.portfolio.positions[stock].market_value
        
        if curr_value > max_value:
            logger.info('stock({}) current value({}) more than max value({})!'.format(stock, curr_value, max_value))
            del context.maxvalue[stock]
            context.maxvalue = pd.concat([context.maxvalue, pd.DataFrame({str(stock): [curr_value]})], axis=1, join='inner')
        else:
            drawdown = (max_value-curr_value)/max_value
            if drawdown < context.drawdown:
                logger.info('stock({}) currnet value({}) less than max value({}) and drawdown is ({})!'.format(stock, curr_value, max_value, drawdown))
            else:
                logger.warn('stock({}) current value({}) less than max value({}) but drawdown is ({})!'.format(stock, curr_value, max_value, drawdown))
                order_obj = order_target_percent(stock, 0)
                if order_obj and order_obj.status != ORDER_STATUS.REJECTED:
                    record(context, stock, 'sell')
        logger.info('stock({}) total income is: {}!'.format(stock, curr_value - context.portfolio.positions[stock].avg_price*context.portfolio.positions[stock].quantity))
    
def rebalance(context, bar_dict):
    holding_stocks = set(get_holding_stocks(context))
    candicate_stocks = set(context.candicate_stocks)
    
    to_sell = holding_stocks - candicate_stocks
    logger.info('stocks to sell are {}!'.format(to_sell))
    for stock in to_sell:
        order_obj = order_target_percent(stock, 0)
        if order_obj and order_obj.status != ORDER_STATUS.REJECTED:
            record(context, stock, 'sell')
    
    to_buy = candicate_stocks - holding_stocks
    logger.info('stocks to buy are {}!'.format(to_buy))
    if to_buy:
        buy_value = float(context.portfolio.cash*0.9)/len(to_buy)
        for stock in to_buy:
            order_obj = order_value(stock, buy_value)
            if order_obj and order_obj.status != ORDER_STATUS.REJECTED:
                record(context, stock, 'buy')
    
def get_holding_stocks(context):
    positions = context.portfolio.positions
    holding_stocks = []
    for stock in positions:
        if context.portfolio.positions[stock].quantity > 0:
            holding_stocks.append(stock)
    return holding_stocks
    
def before_trading(context):
    # 二八轮动判断候选股类型
    judge_2_8(context)

def judge_dividend_yield(context, candicate_stock_list):
    # 股息率原则判断
    fundamental_df = get_fundamentals(
        query(
            fundamentals.eod_derivative_indicator.dividend_yield,
            fundamentals.financial_indicator.inc_operating_revenue,
            fundamentals.eod_derivative_indicator.market_cap
        ).filter(
            fundamentals.financial_indicator.inc_operating_revenue > context.inc_operating_revenue_min
        ).filter(
            fundamentals.eod_derivative_indicator.dividend_yield > context.dividend_yield_min
        ).filter(
            fundamentals.income_statement.stockcode.in_(candicate_stock_list)
        ).order_by(
            fundamentals.eod_derivative_indicator.dividend_yield.desc()
        ).limit(
            context.candicate_num
        )
    )
    # logger.info('fundamental_df is {}!'.format(fundamental_df))
    
    # 当年没有股息率（验证了一下不会出现，不考虑）
    # fundamental_df_nan = get_fundamentals(
    #     query(
    #         fundamentals.eod_derivative_indicator.dividend_yield,
    #         fundamentals.financial_indicator.inc_operating_revenue,
    #         fundamentals.eod_derivative_indicator.market_cap
    #     ).filter(
    #         fundamentals.financial_indicator.inc_operating_revenue > context.inc_operating_revenue_min
    #     ).filter(
    #         fundamentals.eod_derivative_indicator.dividend_yield == np.NAN
    #     ).filter(
    #         fundamentals.income_statement.stockcode.in_(candicate_stock_list)
    #     )
    # )
    # logger.info('fundamental_df_nan is {}!'.format(fundamental_df_nan))
    
    context.candicate_stocks = list(fundamental_df.columns.values)
    if 0 == len(fundamental_df.columns.values):
        # start_ind = randint(0, len(context.gz_stock_list)) - context.candicate_num
        # context.candicate_stocks = list(context.gz_stock_list[start_ind:start_ind+context.candicate_num])
        context.candicate_stocks = context.gz_stock_list
    logger.info('judge by dividend yield, candicated stocks are: {}!'.format(context.candicate_stocks))
    

def operate_2_8(context, bar_dict):
    # 二八轮动操作
    if context.candicate_type == "CSI300.INDX":
        context.hold_type = context.candicate_type
        return judge_dividend_yield(context, context.hs_stock_list)
    elif context.candicate_type == "CSI500.INDX":
        context.hold_type = context.candicate_type
        return judge_dividend_yield(context, context.zz_stock_list)
    else:
        # context.candicate_stocks = context.gz_stock_list
        return judge_dividend_yield(context, context.gz_stock_list)


def handle_bar(context, bar_dict):
    # 二八轮动
    operate_2_8(context, bar_dict)

def judge_2_8(context):
    # 二八轮动判断
    hs300 = history_bars("CSI300.INDX", context.time_span, "1d", "close")
    zz500 = history_bars("CSI500.INDX", context.time_span, "1d", "close")
    hsIncrease = hs300[19] - hs300[0]
    zzIncrease = zz500[19] - zz500[0]
    if hsIncrease < 0 and zzIncrease < 0:
        logger.warn('hsIncrease({}) and zzIncrease({}) all less than 0!'.format(hsIncrease, zzIncrease))
        context.candicate_type = "000012.XSHG"
    elif hsIncrease < zzIncrease:
        logger.warn('hsIncrease({}) less than zzIncrease({})!'.format(hsIncrease, zzIncrease))
        context.candicate_type = "CSI500.INDX"
    else:
        logger.warn('hsIncrease({}) more than zzIncrease({})!'.format(hsIncrease, zzIncrease))
        context.candicate_type = "CSI300.INDX"

def after_trading(context):
    pass
    
