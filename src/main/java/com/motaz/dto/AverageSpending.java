package com.motaz.dto;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class AverageSpending {

    private String customerId;
    private double totalAmount;
    private long transactionCount;

    public double getAverageSpending() {
        return transactionCount == 0 ? 0 : totalAmount / transactionCount;
    }
}

