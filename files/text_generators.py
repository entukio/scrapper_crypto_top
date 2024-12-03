def return_market_summary(total):
    latest = total.iloc[-1]
    total_vs_short_ma_for_text_gen = latest['Total1'] > latest['EMA23']
    total_vs_middle_ma_for_text_gen = latest['Total1'] > latest['EMA56']
    total_vs_long_ma_for_text_gen = latest['Total1'] > latest['SMA200']
    trend_for_text_gen = latest['EMA23'] > latest['EMA56']
    trend_for_text_gen
    text_token_1 = 'No market summary available at the moment'
    text_token_2 = ''

    #4
    if total_vs_short_ma_for_text_gen and not total_vs_middle_ma_for_text_gen and not total_vs_long_ma_for_text_gen:
        output_text = 'Short-term bullish sentiment arose as market cap is above the short moving average, but stays below the middle and long moving averages, so more inflows are needed to return to a clear uptrend.'
        text_token_1 = output_text
    
    #3
    if total_vs_short_ma_for_text_gen and total_vs_middle_ma_for_text_gen and not total_vs_long_ma_for_text_gen:
        output_text = 'The market is quite strong at the moment, as the market cap is above both the short and middle moving averages, but it needs to rise above the long moving average to confirm its strength.'
        text_token_1 = output_text
    
    #1
    if total_vs_short_ma_for_text_gen and total_vs_middle_ma_for_text_gen and total_vs_long_ma_for_text_gen:
        output_text = 'The market is strong, with the market cap above the short, middle, and long moving averages.'
        text_token_1 = output_text
    
    #6
    if not total_vs_short_ma_for_text_gen and total_vs_middle_ma_for_text_gen and total_vs_long_ma_for_text_gen:
        output_text = "Recent weakness is observed with the market cap below the short moving average. Overall, the market cap is strong, being above the middle and long moving averages."
        text_token_1 = output_text
    
    #5
    if not total_vs_short_ma_for_text_gen and not total_vs_middle_ma_for_text_gen and total_vs_long_ma_for_text_gen:
        output_text = "Multiple signs of weakness are present, as the market cap is below the short and middle moving averages. However, it is above the long moving average and needs to stay above it to remain bullish."
        text_token_1 = output_text
    
    #2
    if not total_vs_short_ma_for_text_gen and not total_vs_middle_ma_for_text_gen and not total_vs_long_ma_for_text_gen:
        output_text = "The market is clearly bearish, as the market cap is below the short, middle, and long moving averages."
        text_token_1 = output_text
    
    #7
    if not total_vs_short_ma_for_text_gen and total_vs_middle_ma_for_text_gen and not total_vs_long_ma_for_text_gen:
        output_text = "The market cap is above middle moving average, but below short and long. It needs to break above further to confirm bullishness."
        text_token_1 = output_text
    
    #8
    if total_vs_short_ma_for_text_gen and not total_vs_middle_ma_for_text_gen and total_vs_long_ma_for_text_gen:
        output_text = "The market cap is strong short term, and long term, but below middle moving average."
        text_token_1 = output_text
    
    if trend_for_text_gen:
        text_token_2 = 'Bullish sentiment, as the short moving average is above the middle moving average.'
    elif not trend_for_text_gen:
        text_token_2 = 'Bearish sentiment, as the short moving average is below the middle moving average.'

    return [text_token_1,text_token_2]