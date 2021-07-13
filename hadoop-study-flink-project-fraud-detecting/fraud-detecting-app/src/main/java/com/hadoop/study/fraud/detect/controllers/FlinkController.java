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
import com.hadoop.study.fraud.detect.model.RulePayload;
import com.hadoop.study.fraud.detect.services.FlinkRulesService;
import com.hadoop.study.fraud.detect.utils.UtilJson;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "Fink接口")
@RestController
@RequestMapping("/api")
public class FlinkController {

  @Autowired
  private FlinkRulesService flinkRulesService;

  @GetMapping("/rule/sync")
  public void syncRules() {
    Rule rule = createControlRule(RulePayload.ControlType.EXPORT_RULES_CURRENT);
    flinkRulesService.addRule(rule);
  }

  @GetMapping("/state/clear")
  public void clearState() {
    Rule rule = createControlRule(RulePayload.ControlType.CLEAR_STATE_ALL);
    flinkRulesService.addRule(rule);
  }

  private Rule createControlRule(RulePayload.ControlType controlType) {
    RulePayload payload = new RulePayload();
    payload.setRuleState(RulePayload.RuleState.CONTROL);
    payload.setControlType(controlType);
    Rule rule = new Rule();
    rule.setPayload(UtilJson.writeValueAsString(payload));
    return rule;
  }
}
