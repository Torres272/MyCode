package com.torres.dwpublisher.service;

import java.util.Map;

public interface DauService {

    public int getTotal(String date);

    public Map getRealTimeHours(String date);
}
