/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hadoop.study.fraud.detect.datasource;

import com.hadoop.study.fraud.detect.model.Transaction;
import java.math.BigDecimal;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionGenerator implements Runnable {

    private static final long MAX_PAYEE_ID = 100000L;

    private static final long MAX_BENEFICIARY_ID = 100000L;

    private static final double MIN_PAYMENT_AMOUNT = 5d;

    private static final double MAX_PAYMENT_AMOUNT = 20d;

    private final Throttler throttler;

    private final Consumer<Transaction> consumer;

    private volatile boolean running = true;

    public TransactionGenerator(Consumer<Transaction> consumer, int maxRecordsPerSecond) {
        this.consumer = consumer;
        this.throttler = new Throttler(maxRecordsPerSecond);
    }

    private static Transaction.PaymentType paymentType(long transactionId) {
        int name = (int) (transactionId % 2);
        switch (name) {
            case 0:
                return Transaction.PaymentType.CRD;
            case 1:
                return Transaction.PaymentType.CSH;
            default:
                throw new IllegalStateException("");
        }
    }

    public void adjustMaxRecordsPerSecond(long maxRecordsPerSecond) {
        throttler.adjustMaxRecordsPerSecond(maxRecordsPerSecond);
    }

    protected Transaction randomEvent(SplittableRandom rnd) {
        long transactionId = rnd.nextLong(Long.MAX_VALUE);
        long payeeId = rnd.nextLong(MAX_PAYEE_ID);
        long beneficiaryId = rnd.nextLong(MAX_BENEFICIARY_ID);
        double paymentAmountDouble = ThreadLocalRandom.current().nextDouble(MIN_PAYMENT_AMOUNT, MAX_PAYMENT_AMOUNT);
        paymentAmountDouble = Math.floor(paymentAmountDouble * 100) / 100;
        BigDecimal paymentAmount = BigDecimal.valueOf(paymentAmountDouble);

        return Transaction.builder()
            .transactionId(transactionId)
            .payeeId(payeeId)
            .beneficiaryId(beneficiaryId)
            .paymentAmount(paymentAmount)
            .paymentType(paymentType(transactionId))
            .eventTime(System.currentTimeMillis())
            .build();
    }

    @Override
    public final void run() {
        running = true;

        final SplittableRandom random = new SplittableRandom();

        while (running) {
            Transaction transaction = randomEvent(random);
            log.debug("transaction generator {}", transaction);
            consumer.accept(transaction);
            try {
                throttler.throttle();
            } catch (InterruptedException e) {
                log.error("error: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }
        log.info("finished run");
    }

    public final void cancel() {
        log.info("cancelled");
        running = false;
    }
}
