package com.torres.dwpublisher.mapper;


import java.util.List;
import java.util.Map;

public interface DauMapper {

    public int getTotal(String date);

    public List<Map> selectDauTotalHourMap(String date);
}
