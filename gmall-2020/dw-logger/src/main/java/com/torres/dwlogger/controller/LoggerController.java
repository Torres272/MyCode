package com.torres.dwlogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.torres.bean.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController //RestController = controller + resopnsebody
@Slf4j
public class LoggerController {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;


    @GetMapping("test1")
    public String test01() {
        return "success";
    }

    @GetMapping("test2")
    public String test02(@RequestParam("aa") String aa) {
        return aa;
    }

    @PostMapping("log")
    public String logger(@RequestParam("logString") String logStr) {
        // 0 补充时间戳
        JSONObject jsonObject = JSON.parseObject(logStr);
        jsonObject.put("ts", System.currentTimeMillis());
        // 1 落盘 file
        String jsonString = jsonObject.toJSONString();

        log.info(jsonString);


        // 2 推送到kafka
        if ("startup".equals(jsonObject.getString("logtype"))) {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, jsonString);
        } else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, jsonString);
        }

        return "success";

    }

}
