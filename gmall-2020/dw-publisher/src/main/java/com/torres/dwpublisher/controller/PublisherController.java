package com.torres.dwpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.torres.dwpublisher.bean.Option;
import com.torres.dwpublisher.bean.Stat;
import com.torres.dwpublisher.service.DauService;
import com.torres.dwpublisher.service.GmvService;
import com.torres.dwpublisher.service.SaleDetailService;
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

    @Autowired
    GmvService gmvService;

    @Autowired
    SaleDetailService saleDetailService;

    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date){
        int total = dauService.getTotal(date);
        Double amount = gmvService.getTotal(date);

        ArrayList<Map> result = new ArrayList<>();

        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", total);

        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", "2333");


        HashMap<String, Object> gmvMap = new HashMap<>();
        newMidMap.put("id", "order_amount");
        newMidMap.put("name", "新增交易额");
        newMidMap.put("value", amount);


        result.add(dauMap);
        result.add(newMidMap);

        return JSON.toJSONString(result);
    }

    @GetMapping("realtime-hours")
    public String getRealTimeHours(@RequestParam("id") String id,@RequestParam("date") String date){
        //创建集合用户存放查询结果
        HashMap<String, Map> result = new HashMap<>();
        String yesterday = getYesterday(date);
        Map todayMap = null;
        Map yesterdayMap = null;

        if ("dau".equals(id)) {
            //查询当天数据 date:2020-02-19
            todayMap = dauService.getRealTimeHours(date);
            //查询昨日数据 date:2020-02-18
            yesterdayMap = dauService.getRealTimeHours(yesterday);

        } else if ("order_amount".equals(id)) {
            todayMap = gmvService.getHoursGmv(date);
            yesterdayMap = gmvService.getHoursGmv(yesterday);
        }

        //将今天的以及昨天的数据存放至result
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        //将集合转换为字符串返回
        return JSON.toJSONString(result);
    }

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("startpage") Integer startpage,@RequestParam("size") Integer size,@RequestParam("keyword") String keyword){
        //通过service查询数据
        HashMap<String, Object> detailMap = saleDetailService.getSaleDetail(date, startpage, size, keyword);

        //获取saleDetail各项数据
        Long total = (Long)detailMap.get("total");
        List detail = (List)detailMap.get("detail");

        //获取性别聚合，并进行解析
        Map genderMap = (Map)detailMap.get("gender");
        Long femaleCount = (Long) genderMap.get("F");
        double femaleRatio = Math.round(femaleCount * 1000 / total) / 10D;
        double maleRatio = 100D - femaleRatio;

        Option maleOp = new Option("男", maleRatio);
        Option femaleOp = new Option("女", femaleRatio);

        ArrayList<Option> genderList = new ArrayList<>();
        genderList.add(maleOp);
        genderList.add(femaleOp);
        Stat genderStat = new Stat(genderList, "用户性别占比");

        //获取年龄聚合，并进行解析
        Map ageMap = (Map)detailMap.get("age");

        Long lower20 = 0L;
        Long start20to30 = 0L;

        for (Object age : ageMap.keySet()) {
            Integer age1 = (Integer) age;
            Long ageNum = (Long)ageMap.get(age1);
            if (age1 < 20) {
                lower20 += ageNum;
            } else if (age1 < 30) {
                start20to30 += ageNum;
            }
        }

        double lower20Ratio = Math.round(lower20 * 1000 / total) / 10D;
        double start20to30Ratio = Math.round(start20to30 * 1000 / total) / 10D;
        double upper30Ratio = 100D - lower20Ratio - start20to30Ratio;

        Option lower20Op = new Option("20岁以下", lower20Ratio);
        Option start20to30Op = new Option("20岁到30岁", start20to30Ratio);
        Option upper30Op = new Option("30岁及30岁以上", upper30Ratio);

        ArrayList<Option> ageList = new ArrayList<>();
        ageList.add(lower20Op);
        ageList.add(start20to30Op);
        ageList.add(upper30Op);

        Stat ageStat = new Stat(ageList, "用户年龄占比");


        //存放stat的集合
        ArrayList<Stat> statList = new ArrayList<>();
        statList.add(genderStat);
        statList.add(ageStat);


        //构建最终数据
        HashMap<String, Object> resultMap = new HashMap<>();
        resultMap.put("total", total);
        resultMap.put("stat", statList);
        resultMap.put("detail", detail);

        return JSON.toJSONString(resultMap);
    }

    private static String getYesterday(String date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar instance = Calendar.getInstance();
        try {
            instance.setTime(sdf.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //将当天时间减一
        instance.add(Calendar.DAY_OF_MONTH, -1);
        //2020-02-18
        return sdf.format(new Date(instance.getTimeInMillis()));
    }

}
