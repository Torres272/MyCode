package com.torres.dwpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.torres.dwpublisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    DauService dauService;

    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date){
        int total = dauService.getTotal(date);

        ArrayList<Map> result = new ArrayList<>();

        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", total);

        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", "2333");

        result.add(dauMap);
        result.add(newMidMap);

        return JSON.toJSONString(result);
    }

    @GetMapping("realtime-hours")
    public String getRealTimeHours(@RequestParam("id") String id,@RequestParam("date") String date){
        HashMap<String, Map> result = new HashMap<>();

        Map todayMap = dauService.getRealTimeHours(date);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Calendar instance = Calendar.getInstance();

        try {
            instance.setTime(sdf.parse(date));
        }catch (ParseException e){
            e.printStackTrace();
        }

        instance.add(Calendar.DAY_OF_MONTH, -1);

        String yesterday = sdf.format(new Date(instance.getTimeInMillis()));

        Map yesterdayMap = dauService.getRealTimeHours(yesterday);

        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSON.toJSONString(result);
    }

}
