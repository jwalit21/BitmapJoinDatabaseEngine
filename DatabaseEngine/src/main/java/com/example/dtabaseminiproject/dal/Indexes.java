package com.example.dtabaseminiproject.dal;

import java.util.*;

public class Indexes {
    public Map<Object, String> generateBitmapIndex(List<Map<String, Object>> data, String columnName) {
        Set<Object> distinctValues = new TreeSet<>();

        for (Map<String, Object> row : data) {
            // System.out.println(row);
            Object columnValue = row.get(columnName);
            if (columnValue != null && !distinctValues.contains(columnValue)) {
                distinctValues.add(columnValue);
            }
        }

        Map<Object, String> bitmapIndex = new HashMap<>();
        for(Object val: distinctValues) {
            bitmapIndex.put(val, getIndexValue(data, val, columnName));
        }

        //bitmap index is coming fine 
        // System.out.println(bitmapIndex);
        return bitmapIndex;
    }

    private String getIndexValue(List<Map<String, Object>> data, Object columnValue, String columnName) {
        String indexValue = "";
        for(Map<String, Object> row: data) {
            if (row.get(columnName).equals(columnValue)) {
                indexValue += "1";
            } else {
                indexValue += "0";
            }
        }
        // System.err.println(columnName + " : " + columnValue + " " + indexValue);
        return indexValue;
    }
}
