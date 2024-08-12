package com.stockmarket;

public interface StockDataSource {
    String fetchStockData(String symbol);
    String getSourceName();
}