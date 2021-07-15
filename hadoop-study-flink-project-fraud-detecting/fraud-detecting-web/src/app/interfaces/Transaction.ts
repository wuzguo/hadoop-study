export interface Transaction {
  transactionId: number;
  eventTime: number;
  beneficiaryId: number;
  payeeId: number;
  paymentAmount: number;
  paymentType: string;
}
