package com.stockmarket.strategies;

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
        double price = data.getDouble("price");  // Changed from "close" to "price"

        updateQueue(getQueue(state, "shortTerm", shortTermPeriod), price);
        updateQueue(getQueue(state, "longTerm", longTermPeriod), price);

        super.updateState(symbol, state);
    }

    private Queue<Double> getQueue(JSONObject state, String key, int maxSize) {
        if (!state.has(key)) {
            state.put(key, new JSONObject().put("queue", new LinkedList<Double>()).put("maxSize", maxSize));
        }
        return (Queue<Double>) state.getJSONObject(key).get("queue");
    }

    private void updateQueue(Queue<Double> queue, double price) {
        queue.offer(price);
        if (queue.size() > queue.size()) {
            queue.poll();
        }
    }

    private double calculateAverage(Queue<Double> queue) {
        return queue.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }
}