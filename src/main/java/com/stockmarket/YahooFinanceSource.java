package com.stockmarket;

import org.json.JSONObject;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;



public class YahooFinanceSource implements StockDataSource {
    private static final String BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/%s?interval=1d";

    @Override
    public String fetchStockData(String symbol) {
        try {
            String urlString = String.format(BASE_URL, symbol);
            URL url = new URL(urlString);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
            con.disconnect();
            System.out.println("Yahoo Finance raw response for " + symbol + ": " + content.toString());

            JSONObject json = new JSONObject(content.toString());
            if (!json.has("chart") || !json.getJSONObject("chart").has("result") || json.getJSONObject("chart").getJSONArray("result").length() == 0) {
                System.out.println("Unexpected Yahoo Finance response structure: " + json.toString());
                return null;
            }

            JSONObject result = json.getJSONObject("chart").getJSONArray("result").getJSONObject(0);
            JSONObject meta = result.getJSONObject("meta");
            JSONObject quote = result.getJSONObject("indicators").getJSONArray("quote").getJSONObject(0);

            double open = quote.getJSONArray("open").getDouble(0);
            double high = quote.getJSONArray("high").getDouble(0);
            double low = quote.getJSONArray("low").getDouble(0);
            double price = meta.getDouble("regularMarketPrice");
            long volume = quote.getJSONArray("volume").getLong(0);
            double previousClose = meta.getDouble("chartPreviousClose");

            String stockData = String.format("Open: %.2f, High: %.2f, Low: %.2f, Price: %.2f, Volume: %d, Previous Close: %.2f", open, high, low, price, volume, previousClose);

            System.out.println("Yahoo Finance data for " + symbol + ":\n" + stockData);

            return stockData;

        } catch (Exception e) {
            System.err.println("Error fetching data from Yahoo Finance for " + symbol + ": " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String getSourceName() {
        return "YahooFinance";
    }
}