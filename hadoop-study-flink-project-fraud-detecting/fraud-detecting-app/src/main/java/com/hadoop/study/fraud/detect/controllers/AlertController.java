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

import com.hadoop.study.fraud.detect.entities.Rule;
import com.hadoop.study.fraud.detect.exceptions.NotFoundException;
import com.hadoop.study.fraud.detect.model.AlertEvent;
import com.hadoop.study.fraud.detect.model.Transaction;
import com.hadoop.study.fraud.detect.repositories.RuleRepository;
import com.hadoop.study.fraud.detect.services.KafkaTransactionPusher;
import com.hadoop.study.fraud.detect.utils.UtilJson;
import io.swagger.annotations.Api;
import java.math.BigDecimal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "警报接口")
@Slf4j
@RestController
@RequestMapping("/api")
public class AlertController {

  @Autowired private RuleRepository repository;

  @Autowired private KafkaTransactionPusher transactionsPusher;

  @Autowired private SimpMessagingTemplate simpTemplate;

  @Value("${web-socket.topic.alerts}")
  private String alertsWebSocketTopic;

  @GetMapping("/rules/{ruleId}/alert")
  public AlertEvent mockAlert(@PathVariable Integer ruleId) {
    log.info("mock alert ruleId: {}", ruleId);
    Rule rule = repository.findById(ruleId).orElseThrow(() -> new NotFoundException(ruleId));
    Transaction triggerEvent = transactionsPusher.getLastTransaction();
    BigDecimal triggerValue = triggerEvent.getPaymentAmount().multiply(BigDecimal.valueOf(10));
    AlertEvent alertEvent =
        new AlertEvent(rule.getRuleId(), rule.getPayload(), triggerEvent, triggerValue);
    String alertJson = UtilJson.toJson(alertEvent);
    log.info("mock alert alertJson: {}", alertJson);
    simpTemplate.convertAndSend(alertsWebSocketTopic, alertJson);
    return alertEvent;
  }
}
