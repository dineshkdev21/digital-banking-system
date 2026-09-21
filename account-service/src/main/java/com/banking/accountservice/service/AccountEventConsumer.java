package com.banking.accountservice.service;


import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
public class AccountEventConsumer {


    private final AccountService accountService;

    public AccountEventConsumer(AccountService accountService) {
        this.accountService = accountService;
    }

    /**
     * Consume transaction. completed event from kafka
     * Credits receiver account
     * @param payload
     */
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload){

        try {
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Crediting account: {} amount: {}",receiverAccount,amount);
            accountService.creditBalance(receiverAccount,amount);
        }
        catch (Exception e){
            log.error("Error Crediting account: {}",e.getMessage());
        }
    }


    /**
     * Consume fraud. Detected event from kafka
     * Blocks the flagged account.
     * @param payload
     */
    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected - blocking account: {}",accountNumber);

            accountService.blockAccount(accountNumber);
        }
        catch (Exception e){
            log.error("Error blocking account: {}",e.getMessage());
        }
    }
}
