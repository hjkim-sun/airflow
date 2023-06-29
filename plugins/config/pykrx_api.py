from pykrx import stock


def get_prompt_for_chatgpt(yyyymmdd, market, cnt):
    ticker_name_lst = []
    fluctuation_rate_lst = []
    return_prompt_lst = []
    ohlcv_df = stock.get_market_ohlcv(date=yyyymmdd, market=market) # (market: KOSPI/KOSDAQ/KONEX/ALL)
    fund_df = stock.get_market_fundamental(yyyymmdd, market=market)

    tot_df = ohlcv_df.join(fund_df, how='inner')
    tot_df = tot_df.sort_values(by=['등락률'], ascending=False)
    tot_df.reset_index(inplace=True)

    for idx, row in tot_df.iterrows():
        ticker_name = stock.get_market_ticker_name(row['티커'])
        fluc_rate = row['등락률']
        open_value = row['시가']
        high_value = row['고가']
        low_value = row['저가']
        end_value = row['종가']
        volume = row['거래량']
        bps = row['BPS']
        per = row['PER']
        pbr = row['PBR']
        eps = row['EPS']
        div = row['DIV']
        dps = row['DPS']
        chatgpt_prompt = f'''
        오늘 KOSPI에서 {round(fluc_rate, 2)}%로 상승으로 마감한 {ticker_name}에 대한 정보야.
        {ticker_name}에 대한 회사 소개를 리포트로 만들어줘.
        그리고 아래 정보들도 포함해서 리포트로 만들어줘.
        등락률: {round(fluc_rate, 2)}
        시가: {open_value}
        고가: {high_value}
        저가: {low_value}
        종가: {end_value}
        거래량: {volume}
        BPS: {bps}
        PER: {per}
        PBR: {pbr}
        EPS: {eps}
        DIV: {div}
        DPS: {dps}
        '''
        
        ticker_name_lst.append(ticker_name)
        return_prompt_lst.append(chatgpt_prompt)
        fluctuation_rate_lst.append(fluc_rate)

        if idx >= cnt-1:
            break
    return ticker_name_lst, fluctuation_rate_lst, return_prompt_lst