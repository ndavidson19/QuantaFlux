package com.stockmarket.strategies;

import org.json.JSONObject;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractStrategy implements TradingStrategy {
    protected final String name;
    protected final Map<String, JSONObject> state;

    public AbstractStrategy(String name) {
        this.name = name;
        this.state = new HashMap<>();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void updateState(String symbol, JSONObject data) {
        state.put(symbol, data);
    }

    protected JSONObject getState(String symbol) {
        return state.getOrDefault(symbol, new JSONObject());
    }
}