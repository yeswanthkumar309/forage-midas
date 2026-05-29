package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaTransactionListener {

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate = new RestTemplate();

    public KafkaTransactionListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(Transaction transaction) {

        UserRecord sender =
                databaseConduit.findUser(transaction.getSenderId());

        UserRecord recipient =
                databaseConduit.findUser(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
        );

        float incentiveAmount = 0;

        if (incentive != null) {
            incentiveAmount = incentive.getAmount();
        }

        sender.setBalance(
                sender.getBalance() - transaction.getAmount());

        recipient.setBalance(
                recipient.getBalance()
                        + transaction.getAmount()
                        + incentiveAmount);

        databaseConduit.saveUser(sender);
        databaseConduit.saveUser(recipient);
        if (sender.getName().equals("wilbur")) {
                System.out.println("WILBUR = " + sender.getBalance());
        }
        if (recipient.getName().equals("wilbur")) {
                System.out.println("WILBUR = " + recipient.getBalance());
        }

        databaseConduit.saveTransaction(
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount(),
                        incentiveAmount
                )
        );
    }
}