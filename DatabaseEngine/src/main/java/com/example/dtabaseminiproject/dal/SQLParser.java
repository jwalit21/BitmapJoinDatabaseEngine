package com.example.dtabaseminiproject.dal;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// for bitmap indexing....this program will produce instruction for postgres with columns(within the query) that need to be indexed....
public class SQLParser {
    public List<List<String>> matchingIndexes;

    public SQLParser()
    {
        matchingIndexes = new ArrayList<>();
    }

    public Map<String, List<String>> parser(String query) {
        ArrayList<String> al = new ArrayList<>();
        ArrayList<String> cols = new ArrayList<>();
        // String query = "select supplier_name from suppliers, routes where supplier = suppliers.supplier_id AND region_to = 5;";

        // Extracting conditions after WHERE clause using regex
        Pattern pattern = Pattern.compile("where\\s+(.*?)\\s*;");
        Matcher matcher = pattern.matcher(query.toLowerCase());
        Map<String, List<String>> map = new HashMap<>();


        if (matcher.find()) {
            String conditions = matcher.group(1);
            String[] tokens = conditions.split("\\s+");

            for (int i = 0; i < tokens.length; i++) {
                if (tokens[i].equals("=")) {
                    List<String> temp = new ArrayList<>();
                    if(tokens[i-1].split("\\.").length > 1) {
                        String leftColumn = tokens[i - 1].split("\\.")[1];
                        String leftTable = tokens[i - 1].split("\\.")[0];

                        al.add("create index" +" idx_"+leftColumn+" on "+leftColumn+ " ("+leftTable+")");
                        if(map.containsKey(leftTable)) {
                            map.get(leftTable).add(leftColumn);
                        } else {
                            List<String> l = new ArrayList<>();
                            l.add(leftColumn);
                            map.put(leftTable, l);
                        }
                        temp.add(leftTable + "_" + leftColumn);
                    }
                    else {
                        String leftColumn = tokens[i - 1];
                        String leftTable = implicitTableName(query);

                        al.add("create index" +" idx_"+leftColumn+" on "+leftColumn+ " ("+leftTable+")");
                        if(map.containsKey(leftTable)) {
                            map.get(leftTable).add(leftColumn);
                        } else {
                            List<String> l = new ArrayList<>();
                            l.add(leftColumn);
                            map.put(leftTable, l);
                        }
                        temp.add(leftTable + "_" + leftColumn);
                    }
                    
                    if(tokens[i+1].split("\\.").length > 1) {
                        String rightColumn = tokens[i + 1].split("\\.")[1];
                        String rightTable = tokens[i + 1].split("\\.")[0];
                    
                        al.add("create index" +" idx_"+rightColumn+" on "+rightColumn+ " ("+rightTable+")");
                        if(map.containsKey(rightTable)) {
                            map.get(rightTable).add(rightColumn);
                        } else {
                            List<String> l = new ArrayList<>();
                            l.add(rightColumn);
                            map.put(rightTable, l);
                        }
                        temp.add(rightTable + "_" + rightColumn);
                    }
                    
                    matchingIndexes.add(temp);
                }
            }
        }
        // System.out.println(cols); //feed it to postgres to create index
        return map;
    }

    public List<List<String>> getMatchingIndexes() {
        return matchingIndexes;
    }

    public String implicitTableName(String query) {
        String beforeWhere = query.split("where", 2)[0];
        String[] words = beforeWhere.split("\\s+");
        String lastWord = words[words.length - 1];
        String table = lastWord.trim();
        return table;
    } 
}
