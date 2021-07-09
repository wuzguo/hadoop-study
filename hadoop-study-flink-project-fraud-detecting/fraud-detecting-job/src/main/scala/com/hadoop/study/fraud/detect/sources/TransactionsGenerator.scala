package com.hadoop.study.fraud.detect.sources

import com.hadoop.study.fraud.detect.dynamic.PaymentType.PaymentType
import com.hadoop.study.fraud.detect.dynamic.{PaymentType, Transaction}

import java.util.SplittableRandom
import java.util.concurrent.ThreadLocalRandom

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:50
 */

case class TransactionsGenerator(override var maxRecordsPerSecond: Int) extends BaseGenerator[Transaction] {

    private val MAX_PAYEE_ID = 100000
    private val MAX_BENEFICIARY_ID = 100000

    private val MIN_PAYMENT_AMOUNT = 5d
    private val MAX_PAYMENT_AMOUNT = 20d

    override def randomEvent(splitRandom: SplittableRandom, id: Long): Transaction = {

        val transactionId = splitRandom.nextLong(Long.MaxValue)
        val payeeId = splitRandom.nextLong(MAX_PAYEE_ID)
        val beneficiaryId = splitRandom.nextLong(MAX_BENEFICIARY_ID)
        var paymentAmountDouble = ThreadLocalRandom.current.nextDouble(MIN_PAYMENT_AMOUNT, MAX_PAYMENT_AMOUNT)
        paymentAmountDouble = Math.floor(paymentAmountDouble * 100) / 100
        val paymentAmount = BigDecimal.valueOf(paymentAmountDouble)

        Transaction(transactionId,
            System.currentTimeMillis,
            payeeId,
            beneficiaryId,
            paymentAmount,
            paymentType(transactionId),
            System.currentTimeMillis)
    }


    private def paymentType(id: Long): PaymentType = {
        val name = (id % 2).toInt
        name match {
            case 0 =>
                PaymentType.CRD
            case 1 =>
                PaymentType.CSH
            case _ =>
                throw new IllegalStateException("")
        }
    }
}