package com.stockmarket.strategies;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.LinkedList;
import java.util.Queue;

public class MovingAverageCrossoverStrategy extends AbstractStrategy {
    private final int shortTermPeriod;
    private final int longTermPeriod;

    public MovingAverageCrossoverStrategy(int shortTermPeriod, int longTermPeriod) {
        super("MovingAverageCrossover");
        this.shortTermPeriod = shortTermPeriod;
        this.longTermPeriod = longTermPeriod;
    }

    @Override
    public JSONObject evaluate(String symbol, JSONObject data) {
        JSONObject state = getState(symbol);
        Queue<Double> shortTermPrices = getQueue(state, "shortTerm", shortTermPeriod);
        Queue<Double> longTermPrices = getQueue(state, "longTerm", longTermPeriod);

        if (shortTermPrices.size() == shortTermPeriod && longTermPrices.size() == longTermPeriod) {
            double shortTermMA = calculateAverage(shortTermPrices);
            double longTermMA = calculateAverage(longTermPrices);

            String signal = (shortTermMA > longTermMA) ? "BUY" : "SELL";

            JSONObject signalData = new JSONObject();
            signalData.put("symbol", symbol);
            signalData.put("signal", signal);
            signalData.put("shortTermMA", shortTermMA);
            signalData.put("longTermMA", longTermMA);
            signalData.put("currentPrice", data.getDouble("price"));
            signalData.put("timestamp", System.currentTimeMillis());

            return signalData;
        }

        return null;
    }

    @Override
    public void updateState(String symbol, JSONObject data) {
        JSONObject state = getState(symbol);
        double price = data.getDouble("price");

        updateQueue(getQueue(state, "shortTerm", shortTermPeriod), price);
        updateQueue(getQueue(state, "longTerm", longTermPeriod), price);

        super.updateState(symbol, state);
    }

    private Queue<Double> getQueue(JSONObject state, String key, int maxSize) {
        Queue<Double> queue;

        if (!state.has(key)) {
            queue = new LinkedList<>();
            state.put(key, new JSONArray());
        } else {
            queue = jsonArrayToQueue(state.getJSONArray(key));
        }

        return queue;
    }

    private void updateQueue(Queue<Double> queue, double price) {
        queue.offer(price);
        if (queue.size() > shortTermPeriod) {
            queue.poll();
        }
    }

    private double calculateAverage(Queue<Double> queue) {
        return queue.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }

    private Queue<Double> jsonArrayToQueue(JSONArray jsonArray) {
        Queue<Double> queue = new LinkedList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            queue.add(jsonArray.getDouble(i));
        }
        return queue;
    }

    private JSONArray queueToJsonArray(Queue<Double> queue) {
        JSONArray jsonArray = new JSONArray();
        for (Double value : queue) {
            jsonArray.put(value);
        }
        return jsonArray;
    }
}
