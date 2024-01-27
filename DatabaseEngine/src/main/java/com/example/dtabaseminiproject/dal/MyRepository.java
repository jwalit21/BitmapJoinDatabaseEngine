package com.example.dtabaseminiproject.dal;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.Map.Entry;

//import org.springframework.data.jpa.repository.JpaRepository;
@Repository
public class MyRepository {
    @Autowired
    private JdbcTemplate jdbcTemplate;
    private Map<String, List<Map<String, Object>>> tableData = new HashMap<>();
    private RLEConversion rleConversion;
    Runtime runtime;
    long freeMemory;
    long totalMemory;
    long usedMemory;
    long totalTime;
    long start;
    long finish;
    long timeElapsed;


    public MyRepository() {
        rleConversion = new RLEConversion();
        runtime = Runtime.getRuntime();
    }

    static class MapComparator implements Comparator<Map<String, Object>> {
        @Override
        public int compare(Map<String, Object> map1, Map<String, Object> map2) {
            // Assuming "i" values are Integer, adjust the comparison type accordingly
            Integer id1 = (Integer) map1.get("i");
            Integer id2 = (Integer) map2.get("i");

            // Compare based on "id" in ascending order
            return Integer.compare(id1, id2);
        }
    }

    public void getDataAndStoreIndexes() {

        Indexes indexes = new Indexes();
        Map<Object, String> index1, index2, index3, index4;
        List<Map<String, Object>> temp;

        // filling the data up
        start = System.nanoTime();
        temp = executeQueryWithoutDisply("SELECT * FROM " + "R" + ";");
        Collections.sort(temp, new MapComparator());
        tableData.put("R", temp);
        temp = executeQueryWithoutDisply("SELECT * FROM " + "S" + ";");
        Collections.sort(temp, new MapComparator());
        tableData.put("S", temp);
        finish = System.nanoTime();
        timeElapsed = finish - start;
        System.out.println("Loading the table data into the memory : " + String.format("%.4f", (float)timeElapsed/1000000) + "(milliseconds)");
        freeMemory = runtime.freeMemory();
        totalMemory = runtime.totalMemory();
        usedMemory = totalMemory - freeMemory;
        System.out.println("Used Memory: " + usedMemory / (1024) + " KB");
        System.out.println("Free Memory: " + freeMemory / (1024) + " KB");
        System.out.println("Total Memory: " + totalMemory / (1024) + " KB\n\n");

        // drop the indexes of previous iterations if any
        try {
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + "R_A2_Index");
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + "R_A3_Index");
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + "S_B2_Index");
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + "S_B3_Index");
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
        }

        // create the database index on all 4 columns
        start = System.nanoTime();
        index1 = indexes.generateBitmapIndex(tableData.get("R"), "A2");
        index2 = indexes.generateBitmapIndex(tableData.get("R"), "A3");
        index3 = indexes.generateBitmapIndex(tableData.get("S"), "B2");
        index4 = indexes.generateBitmapIndex(tableData.get("S"), "B3");
        finish = System.nanoTime();
        timeElapsed = finish - start;
        System.out.println("Creating all the 4 indexes A2,B2,A3,B3 : " + String.format("%.4f", (float)timeElapsed/1000000) + "(milliseconds)");
        freeMemory = runtime.freeMemory();
        totalMemory = runtime.totalMemory();
        usedMemory = totalMemory - freeMemory;
        System.out.println("Used Memory: " + usedMemory / (1024) + " KB");
        System.out.println("Free Memory: " + freeMemory / (1024) + " KB");
        System.out.println("Total Memory: " + totalMemory / (1024) + " KB\n\n");

        // compress the indexes
        // System.out.println("R_A2_Index");
        // index1.forEach((key,val) -> {
        //     String encoded = rleConversion.encodeToRLE(val);
        //     index1.put(key, encoded);
        //     System.out.println("key : " + key);
        //     System.out.println("Indexed value : " + val);
        //     System.out.println("Compressed value : " + encoded);
        //     System.out.println("Decompressed value : " + rleConversion.decodeFromRLE(encoded) + "\n");
        // });
        // System.out.println("R_A3_Index");
        // index2.forEach((key,val) -> {
        //     String encoded = rleConversion.encodeToRLE(val);
        //     index1.put(key, encoded);
        //     System.out.println("key : " + key);
        //     System.out.println("Indexed value : " + val);
        //     System.out.println("Compressed value : " + encoded);
        //     System.out.println("Decompressed value : " + rleConversion.decodeFromRLE(encoded) + "\n");
        // });
        // System.out.println("S_B2_Index");
        // index3.forEach((key,val) -> {
        //     String encoded = rleConversion.encodeToRLE(val);
        //     index1.put(key, encoded);
        //     System.out.println("key : " + key);
        //     System.out.println("Indexed value : " + val);
        //     System.out.println("Compressed value : " + encoded);
        //     System.out.println("Decompressed value : " + rleConversion.decodeFromRLE(encoded) + "\n");
        // });
        // System.out.println("S_B3_Index");
        // index4.forEach((key,val) -> {
        //     String encoded = rleConversion.encodeToRLE(val);
        //     index1.put(key, encoded);
        //     System.out.println("key : " + key);
        //     System.out.println("Indexed value : " + val);
        //     System.out.println("Compressed value : " + encoded);
        //     System.out.println("Decompressed value : " + rleConversion.decodeFromRLE(encoded) + "\n");
        // });
        // finish = System.currentTimeMillis();
        // timeElapsed = finish - start;
        // System.out.println("Compressing all the indexes : " + timeElapsed);

        // store the database index on all 4 columns with compression
        start = System.nanoTime();
        storeIndex(index1, "R_A2_Index");
        storeIndex(index2, "R_A3_Index");
        storeIndex(index3, "S_B2_Index");
        storeIndex(index4, "S_B3_Index");
        finish = System.nanoTime();
        timeElapsed = finish - start;
        System.out.println("Storing database indexes to the database for the future use WITH COMPRESSION (time consuming, but useful if multiple queries are there with different indexes to use) : " + String.format("%.4f", (float)timeElapsed/1000000) + "(milliseconds)");
        freeMemory = runtime.freeMemory();
        totalMemory = runtime.totalMemory();
        usedMemory = totalMemory - freeMemory;
        System.out.println("Used Memory: " + usedMemory / (1024) + " KB");
        System.out.println("Free Memory: " + freeMemory / (1024) + " KB");
        System.out.println("Total Memory: " + totalMemory / (1024) + " KB\n\n");
    }

    // stores index with compression
    public void storeIndex(Map<Object, String> index, String tableName) {
        try {
            jdbcTemplate.execute("CREATE TABLE " + tableName + " (ann VARCHAR(10), hvalue VARCHAR(10000000));");
            // UNCOMMENT WHEN DEMO
            // System.out.println("For the Index : " + tableName);
            index.forEach((key, val) -> {
                String encodedRLE = rleConversion.encodeToRLE(val);
                String sql = "INSERT INTO " + tableName + " (ann, hvalue) VALUES (?, ?);";
                jdbcTemplate.update(sql, key, encodedRLE);
                // UNCOMMENT WHEN DEMO
                // System.out.println("key : " + key);
                // System.out.println("Indexed value : " + val);
                // System.out.println("Compressed value : " + encodedRLE);
                // System.out.println("Decompressed value : " + rleConversion.decodeFromRLE(encodedRLE) + "\n");
            });
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
        }
    }

    public List<Map<String, Object>> executeQueryWithoutDisply(String query) {
        List<Map<String, Object>> data = null;
        try {
            data = jdbcTemplate.queryForList(query);
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
        }
        return data;
    }

    // NOT IN USE FOR THIS PROJECT-2
    public void executeQuery(String query) {
        try {
            String sqlQuery = enrichRawQuery(query);
            long start = System.currentTimeMillis();
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sqlQuery);
            long finish = System.currentTimeMillis();
            long timeElapsed = finish - start;
            displayTable(rows);
            System.out.println("\u001B[32m" +String.format(" Time elapsed %d ms",timeElapsed));
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
        }
    }

    // NOT IN USE FOR THIS PROJECT-2
    private void displayTable(List<Map<String, Object>> rows){
        Set<String> columns = rows.get(0).keySet();
        // Calculate column widths
        Map<String, Integer> columnWidths = new HashMap<>();
        for (String column : columns) {
            int maxLength = column.length();
            for (Map<String, Object> row : rows) {
                Object value = row.get(column);
                if (value != null) {
                    int valueLength = value.toString().length();
                    if (valueLength > maxLength) {
                        maxLength = valueLength;
                    }
                }
            }
            columnWidths.put(column, maxLength + 2); // Add padding
        }
        // Print header
        for (String column : columns) {
            System.out.printf("%-" + columnWidths.get(column) + "s | ", column);
        }
        System.out.println();
        // Print separator line
        for (String column : columns) {
            System.out.print("-".repeat(columnWidths.get(column)) + "+");
        }
        System.out.println();
        // Print rows
        for (Map<String, Object> row : rows) {
            for (String column : columns) {
                Object value = row.get(column);
                System.out.printf("%-" + columnWidths.get(column) + "s | ", value);
            }
            System.out.println();
        }
    }
    
    // NOT IN USE FOR THIS PROJECT-2
    private String enrichRawQuery(String query) {
        try {
            query = query.toLowerCase();
            String[] columns = query.split("from")[0].split("select")[1].split(",");
            String[] tables = query.split("from")[1].split("where")[0].split(",");
            ArrayList<String> aliasTables = new ArrayList<>();
            ArrayList<String> aliasColumns = new ArrayList<>();
            aliasColumns.addAll(Arrays.stream(columns).toList());
            String concat="";
            for (int i = 0; i < tables.length; i++) {
                if (tables[i].contains(" as ")) {
                    String tableAliasName = tables[i].split(" as ")[1];
                    aliasTables.add(tables[i]);
                    concat+=String.format(",%s.ann,'+'", tableAliasName.trim());
                } else {
                    aliasTables.add(tables[i]);
                    //add annotation column
                    concat+=String.format(",%s.ann,'+'", tables[i].trim());
                }
            }
            if(concat.length()>0){
                concat=String.format("concat(%s) as bag",concat.substring(1,concat.length()-4));
                aliasColumns.add(concat);
            }
            //just to be safe avoid ; as it's a SQL grammer error for inner queries
            String whereClause=query.split("where")[1].trim();
            if(whereClause.contains(";")){
                whereClause=whereClause.substring(0,whereClause.length()-1);
            }
            String innerQuery=String.format("select %s from %s where %s", String.join(",", aliasColumns), String.join(",", aliasTables),whereClause);
            String aggregator=String.format("select %s, STRING_AGG(bag,' , ') as bag from (%s) group by %s",String.join(",",Arrays.stream(columns).toList()),innerQuery,String.join(",",Arrays.stream(columns).toList()));
            return aggregator;
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
        }
        return "";
    }

    public Map<Object, String> getIndexesFromDB(String tableName, String columnName) {
        List<Map<String, Object>> data;
        Map<Object, String> op = new HashMap<>();
        data = executeQueryWithoutDisply("SELECT * FROM " + tableName+"_"+columnName+"_Index");
        for(Map<String, Object> row: data) {
            op.put(row.get("ann"), rleConversion.decodeFromRLE((String) row.get("hvalue")));
        }
        return op;
    }

    public void executeJoinwithBitmapIndexes(String query) {
        // parsing the query to get map of tables and their columns to get indexed
        SQLParser sqlParser = new SQLParser();
        Map<String, List<String>> tableNamesAndColumns = sqlParser.parser(query);
        List<List<String>> matchingIndexes = sqlParser.getMatchingIndexes();
        Map<String, Map<Object, String>> requiredIndexes = new HashMap<>(); 

        // loading all the required indexes into the memory with DECOMPRESSION
        start = System.nanoTime();
        tableNamesAndColumns.forEach((table, columns) -> {
            for(String col: columns) {
                requiredIndexes.put(table+"_"+col, getIndexesFromDB(table, col));
            }
        });
        finish = System.nanoTime();
        timeElapsed = finish - start;
        System.out.println("Loading the reuired indexes into the memory WITH DECOMPRESSION : " + String.format("%.4f", (float)timeElapsed/1000000) + "(milliseconds)");
        freeMemory = runtime.freeMemory();
        totalMemory = runtime.totalMemory();
        usedMemory = totalMemory - freeMemory;
        System.out.println("Used Memory: " + usedMemory / (1024) + " KB");
        System.out.println("Free Memory: " + freeMemory / (1024) + " KB");
        System.out.println("Total Memory: " + totalMemory / (1024) + " KB\n\n");

        // decompression
        // start = System.currentTimeMillis();
        // requiredIndexes.forEach((key, val) -> {
        //     val.forEach((v, compressed) -> {
        //         requiredIndexes.get(key).put(v, rleConversion.decodeFromRLE(requiredIndexes.get(key).get(v)));
        //     });
        // });
        // finish = System.currentTimeMillis();
        // timeElapsed = finish - start;
        // System.out.println("Decompressing indexes which are in the memory : " + timeElapsed);

        // perform bitwise AND of matching indexes
        start = System.nanoTime();
        List<String> matchingTuples = new ArrayList<>();
        int index1Length = 0, index2Length = 0;
        
        for(List<String> matchingIndex: matchingIndexes) {
            String val1 = matchingIndex.get(0);
            String val2 = matchingIndex.get(1);
            Map<Object, String> index1 = requiredIndexes.get(val1);
            Map<Object, String> index2 = requiredIndexes.get(val2);

            for (Entry<Object, String> entry : index1.entrySet()) {
                String firstValue = entry.getValue();
                index1Length = firstValue.length();
                break;  // Break after the first entry
            }

            for (Entry<Object, String> entry : index2.entrySet()) {
                String firstValue = entry.getValue();
                index2Length = firstValue.length();
                break;  // Break after the first entry
            }
        }

        for(int i = 0; i< index1Length; i++) {
            for(int j=0; j< index2Length; j++) {
                String sNum1 = "", sNum2 = "";

                boolean result = true;
                for(List<String> matchingIndex: matchingIndexes) {
                    String val1 = matchingIndex.get(0);
                    String val2 = matchingIndex.get(1);
                    Map<Object, String> index1 = requiredIndexes.get(val1);
                    Map<Object, String> index2 = requiredIndexes.get(val2);
                    // System.out.println(val1 + " " + index1);
                    // System.out.println(val2 + " " + index2);

                    for (Map.Entry<Object, String> entry : index1.entrySet()) {
                        sNum1 += entry.getValue().charAt(i);
                    }
                    for (Map.Entry<Object, String> entry : index2.entrySet()) {
                        sNum2 += entry.getValue().charAt(j);
                    }

                    // System.out.println("snum1: " + sNum1 + " snum2: " + sNum2);
                    boolean res = bitwiseAnd(sNum1, sNum2);
                    sNum1 = "";
                    sNum2 = "";

                    // true that means and is 0 and it's not matching
                    if (res) {
                        result = false;
                        // System.out.println("not matching " + i + " " + j);
                        break;
                    }
                }
                if(result) {
                    // System.out.println("R" + i + " X S" + j);
                    matchingTuples.add("R" + i + " X S" + j);
                }
            }
        }
        finish = System.nanoTime();
        timeElapsed = finish - start;
        System.out.println("Performing bitwise AND on the indexes which are in memory : " + String.format("%.4f", (float)timeElapsed/1000000) + "(milliseconds)");
        freeMemory = runtime.freeMemory();
        totalMemory = runtime.totalMemory();
        usedMemory = totalMemory - freeMemory;
        System.out.println("Used Memory: " + usedMemory / (1024) + " KB");
        System.out.println("Free Memory: " + freeMemory / (1024) + " KB");
        System.out.println("Total Memory: " + totalMemory / (1024) + " KB\n\n");

        System.out.println("\n\n***    Total matched Tuples : " + matchingTuples.size() + "    ***\n");
    }

    public boolean bitwiseAnd(String num1, String num2) {
        int length = Math.min(num1.length(), num2.length());
        for (int i = 0; i < length; i++) {
            char bit1 = num1.charAt(i);
            char bit2 = num2.charAt(i);
            if(bit1!=bit2) {
                return true;
            }
        }
        return false;
    }
}
