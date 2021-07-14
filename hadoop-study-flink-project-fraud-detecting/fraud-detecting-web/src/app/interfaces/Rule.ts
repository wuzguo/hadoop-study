import { RefObject } from "react";

export interface Rule {
  id: number;
  payload: string;
  ref: RefObject<HTMLDivElement>;
}

export interface RulePayload {
  aggregateFieldName: string;
  aggregatorType: string;
  groupingKeyNames: string[];
  limit: number;
  operatorType: string;
  windowMinutes: number;
  ruleState: string;
}
