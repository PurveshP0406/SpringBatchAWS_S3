package com.example.ISB.Infocrush.Model;

@lombok.Data
public class Data {

    private String transaction_date;
    private String posted_date;
    private String transaction_description;
    private Double debit;
    private Integer credited;
    private Integer balance;
}
