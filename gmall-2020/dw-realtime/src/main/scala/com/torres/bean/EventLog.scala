package com.torres.bean

case class EventLog(mid: String,
                    uid: String,
                    appid: String,
                    area: String,
                    os: String,
                    logType: String,
                    evid: String,
                    pgid: String,
                    npgid: String,
                    itemid: String,
                    var logDate: String,
                    var logHour: String,
                    var ts: Long)
