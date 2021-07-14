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

package com.hadoop.study.fraud.detect.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Rules representation.
 */

@Data
@ToString
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class RulePayload {

    private Integer ruleId;

    private RuleState ruleState;

    /**
     * aggregation
     */
    private List<String> groupingKeyNames;

    private String aggregateFieldName;

    private AggregatorType aggregatorType;

    private OperatorType operatorType;

    private BigDecimal limit;

    private Integer windowMinutes;

    private ControlType controlType;

    /**
     * Evaluates this rule by comparing provided value with rules' limit based on limit operator type.
     *
     * @param comparisonValue value to be compared with the limit
     */
    public boolean apply(BigDecimal comparisonValue) {
        switch (operatorType) {
            case EQUAL:
                return comparisonValue.compareTo(limit) == 0;
            case NOT_EQUAL:
                return comparisonValue.compareTo(limit) != 0;
            case GREATER:
                return comparisonValue.compareTo(limit) > 0;
            case LESS:
                return comparisonValue.compareTo(limit) < 0;
            case LESS_EQUAL:
                return comparisonValue.compareTo(limit) <= 0;
            case GREATER_EQUAL:
                return comparisonValue.compareTo(limit) >= 0;
            default:
                throw new RuntimeException("Unknown limit operator type: " + operatorType);
        }
    }

    public enum AggregatorType {
        SUM,
        AVG,
        MIN,
        MAX
    }

    public enum OperatorType {
        EQUAL("="),
        NOT_EQUAL("!="),
        GREATER_EQUAL(">="),
        LESS_EQUAL("<="),
        GREATER(">"),
        LESS("<");

        String operator;

        OperatorType(String operator) {
            this.operator = operator;
        }

        public static OperatorType typeOf(String text) {
            for (OperatorType type : OperatorType.values()) {
                if (type.operator.equals(text)) {
                    return type;
                }
            }
            return null;
        }
    }

    public enum RuleState {
        ACTIVE,
        PAUSE,
        DELETE,
        CONTROL
    }

    public enum ControlType {
        CLEAR_STATE_ALL,
        DELETE_RULES_ALL,
        EXPORT_RULES_CURRENT
    }
}
