import { Transaction } from "./Transaction";
import { RefObject } from "react";
import { RulePayload } from "./Rule";

export interface Alert {
  alertId: string;
  ruleId: number;
  payload: RulePayload;
  triggerValue: number;
  triggerEvent: Transaction;
  ref: RefObject<HTMLDivElement>;
}
