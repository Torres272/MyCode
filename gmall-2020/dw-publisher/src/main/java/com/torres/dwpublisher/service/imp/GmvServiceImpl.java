package com.torres.dwpublisher.service.imp;

import com.torres.dwpublisher.mapper.GmvMapper;
import com.torres.dwpublisher.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class GmvServiceImpl implements GmvService {

    @Autowired
    GmvMapper gmvMapper;

    @Override
    public Double getTotal(String date) {
        return gmvMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getHoursGmv(String date) {
        List<Map> maps = gmvMapper.selectOrderAmountHourMap(date);

        HashMap<String, Object> map = new HashMap<>();


        maps.forEach(map1->{
            map.put((String) map1.get("CREATE_HOUR"), (Double) map1.get("SUM_AMOUNT"));
        });

        return map;
    }
}
