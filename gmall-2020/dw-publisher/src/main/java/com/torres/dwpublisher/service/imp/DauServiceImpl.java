package com.torres.dwpublisher.service.imp;

import com.torres.dwpublisher.mapper.DauMapper;
import com.torres.dwpublisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DauServiceImpl implements DauService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public int getTotal(String date) {
        return dauMapper.getTotal(date);
    }

    @Override
    public Map getRealTimeHours(String date) {
        List<Map> list = dauMapper.selectDauTotalHourMap(date);
        HashMap<String, Long> map = new HashMap<>();

        list.forEach(item ->map.put((String)item.get("LH"),(Long)item.get("CT")));

        return map;
    }
}
