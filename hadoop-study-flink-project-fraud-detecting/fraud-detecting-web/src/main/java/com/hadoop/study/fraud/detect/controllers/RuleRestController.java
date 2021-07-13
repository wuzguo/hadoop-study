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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hadoop.study.fraud.detect.entities.Rule;
import com.hadoop.study.fraud.detect.exceptions.NotFoundException;
import com.hadoop.study.fraud.detect.model.RulePayload;
import com.hadoop.study.fraud.detect.repositories.RuleRepository;
import com.hadoop.study.fraud.detect.services.FlinkRulesService;
import com.hadoop.study.fraud.detect.utils.UtilJson;
import io.swagger.annotations.Api;
import java.io.IOException;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "规则设置接口")
@RestController
@RequestMapping("/api")
public class RuleRestController {

    @Autowired
    private RuleRepository repository;

    @Autowired
    private FlinkRulesService flinkRulesService;

    @GetMapping("/rules")
    List<Rule> all() {
        return repository.findAll();
    }

    @PostMapping("/rules")
    Rule newRule(@RequestBody Rule newRule) throws IOException {
        Rule savedRule = repository.save(newRule);
        Integer id = savedRule.getId();
        RulePayload payload = UtilJson.readValue(savedRule.getRulePayload(), RulePayload.class);
        payload.setRuleId(id);
        String payloadJson = UtilJson.writeValueAsString(payload);
        savedRule.setRulePayload(payloadJson);
        Rule result = repository.save(savedRule);
        flinkRulesService.addRule(result);
        return result;
    }

    @GetMapping("/rules/pushToFlink")
    void pushToFlink() {
        List<Rule> rules = repository.findAll();
        for (Rule rule : rules) {
            flinkRulesService.addRule(rule);
        }
    }

    @GetMapping("/rules/{id}")
    Rule one(@PathVariable Integer id) {
        return repository.findById(id).orElseThrow(() -> new NotFoundException(id));
    }

    @DeleteMapping("/rules/{id}")
    void deleteRule(@PathVariable Integer id) throws JsonProcessingException {
        repository.deleteById(id);
        flinkRulesService.deleteRule(id);
    }

    @DeleteMapping("/rules")
    void deleteAllRules() throws JsonProcessingException {
        List<Rule> rules = repository.findAll();
        for (Rule rule : rules) {
            repository.deleteById(rule.getId());
            flinkRulesService.deleteRule(rule.getId());
        }
    }
}
