package com.hadoop.study.fraud.detect.dynamic


import com.hadoop.study.fraud.detect.dynamic.PaymentType.PaymentType

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Locale

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 13:50
 */

case class Transaction(transactionId: Long, eventTime: Long, payeeId: Long, beneficiaryId: Long,
                       paymentAmount: BigDecimal, paymentType: PaymentType, var ingestionTimestamp: Long) extends TimestampAssignable[Long] {

    def assignIngestionTimestamp(timestamp: Long): Unit = {
        this.ingestionTimestamp = timestamp
    }
}

object Transaction {

    private val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZone(ZoneOffset.UTC)

    def from(line: String): Transaction = {
        val tokens = line.split(",")
        val numArgs = 7
        if (tokens.size != numArgs) throw new RuntimeException("Invalid transaction: " + line + ". Required number of arguments: " + numArgs + " found " + tokens.size)

        val iter = tokens.iterator
        Transaction(iter.next.toLong,
            ZonedDateTime.parse(iter.next, timeFormatter).toInstant.toEpochMilli,
            iter.next.toLong,
            iter.next.toLong,
            BigDecimal(iter.next),
            PaymentType.withName(iter.next),
            iter.next.toLong
        )
    }
}

object PaymentType extends Enumeration {

    type PaymentType = Value

    val CSH: Value = Value("CSH")

    val CRD: Value = Value("CRD")
}