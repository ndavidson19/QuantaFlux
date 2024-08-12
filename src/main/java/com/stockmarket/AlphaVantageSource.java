package com.stockmarket;

import org.json.JSONObject;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class AlphaVantageSource implements StockDataSource {
    private static final String API_KEY = System.getenv("ALPHA_VANTAGE_API_KEY");
    private static final String BASE_URL = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=%s&apikey=%s";

    @Override
    public String fetchStockData(String symbol) {
        try {
            String urlString = String.format(BASE_URL, symbol, API_KEY);
            URL url = new URL(urlString);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            System.out.println("Alpha Vantage raw response for " + symbol + ": " + content.toString());

            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
            con.disconnect();

            JSONObject json = new JSONObject(content.toString());
            if (json.has("Note")) {
                System.out.println("Alpha Vantage API limit reached: " + json.getString("Note"));
                return null;
            }
            if (!json.has("Global Quote")) {
                System.out.println("Unexpected Alpha Vantage response structure: " + json.toString());
                return null;
            }
            JSONObject globalQuote = json.getJSONObject("Global Quote");

            return new JSONObject()
                .put("symbol", globalQuote.getString("01. symbol"))
                .put("open", globalQuote.getDouble("02. open"))
                .put("high", globalQuote.getDouble("03. high"))
                .put("low", globalQuote.getDouble("04. low"))
                .put("price", globalQuote.getDouble("05. price"))
                .put("volume", globalQuote.getLong("06. volume"))
                .put("latestTradingDay", globalQuote.getString("07. latest trading day"))
                .put("previousClose", globalQuote.getDouble("08. previous close"))
                .put("change", globalQuote.getDouble("09. change"))
                .put("changePercent", globalQuote.getString("10. change percent"))
                .toString();

            } catch (Exception e) {
                System.err.println("Error fetching data from Alpha Vantage for " + symbol + ": " + e.getMessage());
                e.printStackTrace();
                return null;
            }
    }

    @Override
    public String getSourceName() {
        return "AlphaVantage";
    }
}