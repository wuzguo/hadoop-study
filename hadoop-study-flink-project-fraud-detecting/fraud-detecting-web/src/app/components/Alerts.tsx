import React, {FC} from "react";
import {Badge, Button, CardBody, CardFooter, CardHeader, Table} from "reactstrap";
import styled from "styled-components/macro";
import {faArrowRight} from "@fortawesome/free-solid-svg-icons";
import {FontAwesomeIcon} from "@fortawesome/react-fontawesome";

import {Alert} from "../interfaces";
import {CenteredContainer} from "./CenteredContainer";
import {ScrollingCol} from "./App";
import {Beneficiary, Details, Payee, Payment, paymentTypeMap} from "./Transactions";
import {Line} from "app/utils/useLines";

const AlertTable = styled(Table)`
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

export const Alerts: FC<Props> = props => {
  const tooManyAlerts = props.alerts.length > 4;

  const handleScroll = () => {
    props.lines.forEach(line => line.line.position());
  };

  return (
      <ScrollingCol xs={{size: 3, offset: 1}} onScroll={handleScroll}>
        {props.alerts.map((alert, idx) => {
          const t = alert.triggerEvent;
          return (
              <CenteredContainer
                  key={idx}
                  className="w-100"
                  ref={alert.ref}
                  tooManyItems={tooManyAlerts}
                  style={{borderColor: "#ffc107", borderWidth: 2}}
              >
                <CardHeader>
                  Alert
                  <Button size="sm" color="primary" onClick={props.clearAlert(idx)} className="ml-3">
                    Clear Alert
                  </Button>
                </CardHeader>
                <CardBody className="p-0">
                  <AlertTable size="sm" bordered={true}>
                    <tbody>
                    <tr>
                      <td>Transaction</td>
                      <td>{alert.triggerEvent.transactionId}</td>
                    </tr>
                    <tr>
                      <td colSpan={2} className="p-0" style={{borderBottomWidth: 3}}>
                        <Payment className="px-2">
                          <Payee>{t.payeeId}</Payee>
                          <Details>
                            <FontAwesomeIcon className="mx-1" icon={paymentTypeMap[t.paymentType]}/>
                            <Badge color="info">${parseFloat(t.paymentAmount.toString()).toFixed(2)}</Badge>
                            <FontAwesomeIcon className="mx-1" icon={faArrowRight}/>
                          </Details>
                          <Beneficiary>{t.beneficiaryId}</Beneficiary>
                        </Payment>
                      </td>
                    </tr>
                    <tr>
                      <td>Rule</td>
                      <td>{alert.ruleId}</td>
                    </tr>
                    <tr>
                      <td>Amount</td>
                      <td>{alert.triggerValue}</td>
                    </tr>
                    <tr>
                      <td>Of</td>
                      <td>{alert.payload.aggregateFieldName}</td>
                    </tr>
                    </tbody>
                  </AlertTable>
                </CardBody>
                <CardFooter style={{padding: "0.3rem"}}>
                  Alert for Rule <em>{alert.ruleId}</em> caused by Transaction{" "}
                  <em>{alert.triggerEvent.transactionId}</em> with Amount <em>{alert.triggerValue}</em> of{" "}
                  <em>{alert.payload.aggregateFieldName}</em>.
                </CardFooter>
              </CenteredContainer>
          );
        })}
      </ScrollingCol>
  );
};

interface Props {
  alerts: Alert[];
  clearAlert: any;
  lines: Line[];
  // handleScroll: () => void;
}
