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

package com.hadoop.study.fraud.detect.controllers;

import com.hadoop.study.fraud.detect.datasource.DemoTransactionGenerator;
import com.hadoop.study.fraud.detect.datasource.TransactionGenerator;
import com.hadoop.study.fraud.detect.services.KafkaTransactionPusher;
import io.swagger.annotations.Api;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Api(tags = "数据生成接口")
@RestController
@RequestMapping("/api/transaction")
public class TransactionController {

    private final TransactionGenerator transactionGenerator;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private boolean generatingTransactions = false;

    private boolean listenerContainerRunning = true;

    @Value("${kafka.listeners.transactions.id}")
    private String transactionListenerId;

    @Value("${transactionsRateDisplayLimit}")
    private int transactionsRateDisplayLimit;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    public TransactionController(KafkaTransactionPusher transactionsPusher) {
        transactionGenerator = new DemoTransactionGenerator(transactionsPusher, 1);
    }

    @GetMapping("/start")
    public void start() {
        log.info("{}", "start generator Transaction called");
        startGenTransaction();
    }

    private void startGenTransaction() {
        if (!generatingTransactions) {
            executor.submit(transactionGenerator);
            generatingTransactions = true;
        }
    }

    @GetMapping("/stop")
    public void stop() {
        transactionGenerator.cancel();
        generatingTransactions = false;
        log.info("{}", "stop generator transaction called");
    }

    @GetMapping("/speed/{speed}")
    public void setSpeed(@PathVariable Long speed) {
        log.info("generator speed change request: " + speed);
        if (speed <= 0) {
            transactionGenerator.cancel();
            generatingTransactions = false;
            return;
        } else {
            startGenTransaction();
        }

        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(transactionListenerId);
        if (speed > transactionsRateDisplayLimit) {
            listenerContainer.stop();
            listenerContainerRunning = false;
        } else if (!listenerContainerRunning) {
            listenerContainer.start();
        }

        transactionGenerator.adjustMaxRecordsPerSecond(speed);
    }
}
