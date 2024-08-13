package com.stockmarket.strategies;

import org.json.JSONObject;

public interface TradingStrategy {
    String getName();
    JSONObject evaluate(String symbol, JSONObject data);
    void updateState(String symbol, JSONObject data);
}