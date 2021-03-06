import {library} from "@fortawesome/fontawesome-svg-core";
import {
  faArrowUp,
  faCalculator,
  faClock,
  faFont,
  faInfoCircle,
  faLaptopCode,
  faLayerGroup,
  IconDefinition,
} from "@fortawesome/free-solid-svg-icons";
import {FontAwesomeIcon} from "@fortawesome/react-fontawesome";
import Axios from "axios";
import {isArray} from "lodash/fp";
import React, {FC} from "react";

import {Badge, Button, CardBody, CardFooter, CardHeader, Table} from "reactstrap";
import styled from "styled-components/macro";
import {Alert, Rule} from "../interfaces";
import {CenteredContainer} from "./CenteredContainer";
import {ScrollingCol} from "./App";
import {Line} from "app/utils/useLines";

library.add(faInfoCircle);

const badgeColorMap: {
  [s: string]: string;
} = {
  ACTIVE: "success",
  DELETE: "danger",
  PAUSE: "warning",
};

const iconMap: {
  [s: string]: IconDefinition;
} = {
  aggregateFieldName: faFont,
  aggregatorType: faCalculator,
  groupingKeyNames: faLayerGroup,
  limit: faArrowUp,
  operatorType: faLaptopCode,
  windowMinutes: faClock,
};

const separator: {
  [s: string]: string;
} = {
  EQUAL: "to",
  GREATER: "than",
  GREATER_EQUAL: "than",
  LESS: "than",
  LESS_EQUAL: "than",
  NOT_EQUAL: "to",
};

const RuleTitle = styled.div`
  display: flex;
  align-items: center;
`;

const RuleTable = styled(Table)`
  && {
    width: calc(100% + 1px);
    border: 0;
    margin: 0;

    td {
      vertical-align: middle !important;

      &:first-child {
        border-left: 0;
      }

      &:last-child {
        border-right: 0;
      }
    }

    tr:first-child {
      td {
        border-top: 0;
      }
    }
  }
`;

const fields = [
  "aggregatorType",
  "aggregateFieldName",
  "groupingKeyNames",
  "operatorType",
  "limit",
  "windowMinutes",
];

// const omitFields = omit(["ruleId", "ruleState"]);

const hasAlert = (alerts: Alert[], rule: Rule) => alerts.some(alert => alert.ruleId === rule.ruleId);

export const Rules: FC<Props> = props => {
  const handleDelete = (ruleId: number) => () => {
    Axios.delete(`/api/rules/${ruleId}`).then(props.clearRule(ruleId));
  };

  const handleScroll = () => {
    props.ruleLines.forEach(line => line.line.position());
    props.alertLines.forEach(line => line.line.position());
  };

  const tooManyRules = props.rules.length > 3;

  return (
      <ScrollingCol xs={{size: 5, offset: 1}} onScroll={handleScroll}>
        {props.rules.map(rule => {
          const payload = JSON.parse(rule.payload);

          if (!payload) {
            return null;
          }

          return (
              <CenteredContainer
                  ref={rule.ref}
                  key={rule.ruleId}
                  tooManyItems={tooManyRules}
                  style={{
                    borderColor: hasAlert(props.alerts, rule) ? "#dc3545" : undefined,
                    borderWidth: hasAlert(props.alerts, rule) ? 2 : 1,
                  }}
              >
                <CardHeader className="d-flex justify-content-between align-items-center" style={{padding: "0.3rem"}}>
                  <RuleTitle>
                    <FontAwesomeIcon icon={faInfoCircle} fixedWidth={true} className="mr-2"/>
                    Rule #{rule.ruleId}{" "}
                    <Badge color={badgeColorMap[payload.ruleState]} className="ml-2">
                      {payload.ruleState}
                    </Badge>
                  </RuleTitle>
                  <Button size="sm" color="danger" outline={true} onClick={handleDelete(rule.ruleId)}>
                    Delete
                  </Button>
                </CardHeader>
                <CardBody className="p-0">
                  <RuleTable size="sm" bordered={true}>
                    <tbody>
                    {fields.map(key => {
                      const field = payload[key];
                      return (
                          <tr key={key}>
                            <td style={{width: 10}}>
                              <FontAwesomeIcon icon={iconMap[key]} fixedWidth={true}/>
                            </td>
                            <td style={{width: 30}}>{key}</td>
                            <td>{isArray(field) ? field.map(v => `"${v}"`).join(", ") : field}</td>
                          </tr>
                      );
                    })}
                    </tbody>
                  </RuleTable>
                </CardBody>
                <CardFooter style={{padding: "0.3rem"}}>
                  <em>{payload.aggregatorType}</em> of <em>{payload.aggregateFieldName}</em> aggregated by "
                  <em>{payload.groupingKeyNames.join(", ")}</em>" is <em>{payload.operatorType}</em>{" "}
                  {separator[payload.operatorType]} <em>{payload.limit}</em> within an interval of{" "}
                  <em>{payload.windowMinutes}</em> minutes.
                </CardFooter>
              </CenteredContainer>
          );
        })}
      </ScrollingCol>
  );
};

interface Props {
  alerts: Alert[];
  rules: Rule[];
  clearRule: (ruleId: number) => () => void;
  ruleLines: Line[];
  alertLines: Line[];
}
