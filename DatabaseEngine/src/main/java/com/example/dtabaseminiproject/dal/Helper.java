package com.example.dtabaseminiproject.dal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.context.ConfigurableApplicationContext;

public class Helper {
    private ConfigurableApplicationContext context;
    public Helper(ConfigurableApplicationContext context) {
        this.context = context;
    }
    public void createIndex(String table, String column) {
    }
    public List<Map<String, Object>> getData(String table) {
        MyRepository repository = context.getBean(MyRepository.class);
        List<Map<String, Object>> data = repository.executeQueryWithoutDisply("SELECT * FROM " + table + ";");
        // for(int i=0; i< data.size(); i++) {
        //     Map<String, Object> myMap = data.get(i);
        //     myMap.forEach((key, val) -> {
        //         System.out.println(key + " : " + val);
        //     });
        // }
        return data;
    }
}
