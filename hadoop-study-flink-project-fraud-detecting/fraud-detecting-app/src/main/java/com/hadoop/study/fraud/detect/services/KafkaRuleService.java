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
import com.hadoop.study.fraud.detect.model.RulePayload.RuleState;
import com.hadoop.study.fraud.detect.utils.UtilJson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaRuleService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.rules}")
    private String topic;

    public void sendRule(Rule rule) {
        String payload = rule.getPayload();
        kafkaTemplate.send(topic, payload);
    }

    public void deleteRule(int ruleId) {
        RulePayload payload = new RulePayload();
        payload.setRuleId(ruleId);
        payload.setRuleState(RuleState.DELETE);
        String payloadJson = UtilJson.writeValueAsString(payload);
        kafkaTemplate.send(topic, payloadJson);
    }
}
