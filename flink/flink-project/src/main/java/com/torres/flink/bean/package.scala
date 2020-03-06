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

    case class ApacheLog(
                        ip:String,
                        userId:String,
                        eventTime:Long,
                        method:String,
                        url:String
                        )

    case class HotResourceClick(
                                 url:String,
                                 clickCount:Long,
                                 windowEndTime:Long)
}
