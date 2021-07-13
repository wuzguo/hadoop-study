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

package com.hadoop.study.fraud.detect.services;

import com.hadoop.study.fraud.detect.entities.Rule;
import com.hadoop.study.fraud.detect.model.RulePayload;
import com.hadoop.study.fraud.detect.repositories.RuleRepository;
import com.hadoop.study.fraud.detect.utils.UtilJson;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {

  @Autowired private SimpMessagingTemplate simpTemplate;

  @Autowired private RuleRepository ruleRepository;

  @Value("${web-socket.topic.alerts}")
  private String alertsWebSocketTopic;

  @Value("${web-socket.topic.latency}")
  private String latencyWebSocketTopic;

  @KafkaListener(topics = "${kafka.topic.alerts}", groupId = "alerts")
  public void templateAlerts(@Payload String message) {
    log.debug("{}", message);
    simpTemplate.convertAndSend(alertsWebSocketTopic, message);
  }

  @KafkaListener(topics = "${kafka.topic.latency}", groupId = "latency")
  public void templateLatency(@Payload String message) {
    log.debug("{}", message);
    simpTemplate.convertAndSend(latencyWebSocketTopic, message);
  }

  @KafkaListener(topics = "${kafka.topic.current-rules}", groupId = "current-rules")
  public void saveTemplateRules(@Payload String message) {
    log.info("{}", message);
    RulePayload payload = UtilJson.readValue(message, RulePayload.class);
    Integer payloadId = payload.getRuleId();
    Optional<Rule> ruleOptional = ruleRepository.findById(payloadId);
    if (!ruleOptional.isPresent()) {
      ruleRepository.save(new Rule(payloadId, UtilJson.writeValueAsString(payload)));
    }
  }
}
