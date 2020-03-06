package com.torres.flink

package object bean {

    case class UserBehavior(
                             userId: Long,
                             itemId: Long,
                             categoryId: Long,
                             behavior: String,
                             timestamp: Long
                           )

    case class HotItemClick(
                             itemId: Long,
                             clickCount: Long,
                             windowEndTime: Long
                           )

}
