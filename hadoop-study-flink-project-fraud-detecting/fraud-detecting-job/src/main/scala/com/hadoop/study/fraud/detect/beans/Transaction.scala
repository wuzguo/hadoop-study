package com.hadoop.study.fraud.detect.beans

import com.hadoop.study.fraud.detect.dynamic.{JsonMapper2, TimestampAssignable}

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

class Transaction extends TimestampAssignable[Long] with Serializable {

    var transactionId: Long = 0L

    var eventTime: Long = 0L

    var payeeId: Long = 0L

    var beneficiaryId: Long = 0L

    var paymentAmount: BigDecimal = _

    var paymentType: String = _

    var ingestionTimestamp: Long = 0L

    def assignIngestionTimestamp(timestamp: Long): Unit = {
        this.ingestionTimestamp = timestamp
    }

    override def toString: String = JsonMapper2(classOf[Transaction]).to(this)
}

object Transaction {

    private val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZone(ZoneOffset.UTC)

    def from(line: String): Transaction = {
        val tokens = line.split(",")
        val numArgs = 7
        if (tokens.size != numArgs) throw new RuntimeException("Invalid transaction: " + line + ". Required number of arguments: " + numArgs + " found " + tokens.size)

        val iter = tokens.iterator
        val transaction = new Transaction()
        transaction.transactionId = iter.next.toLong
        transaction.eventTime = ZonedDateTime.parse(iter.next, timeFormatter).toInstant.toEpochMilli
        transaction.payeeId = iter.next.toLong
        transaction.beneficiaryId = iter.next.toLong
        transaction.paymentAmount = BigDecimal(iter.next)
        transaction.paymentType = iter.next
        transaction.ingestionTimestamp = iter.next.toLong
        transaction
    }
}

