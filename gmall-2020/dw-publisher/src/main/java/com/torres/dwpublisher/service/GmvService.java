package com.torres.dwpublisher.service;

import java.util.Map;

public interface GmvService {
    public Double getTotal(String date);

    public Map getHoursGmv(String date);
}
