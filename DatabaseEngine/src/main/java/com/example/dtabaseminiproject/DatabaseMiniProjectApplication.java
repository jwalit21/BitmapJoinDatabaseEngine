package com.example.dtabaseminiproject;

import com.example.dtabaseminiproject.dal.Helper;
import com.example.dtabaseminiproject.dal.Indexes;
import com.example.dtabaseminiproject.dal.MyRepository;
import com.example.dtabaseminiproject.dal.SQLParser;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Scanner;

@SpringBootApplication
public class DatabaseMiniProjectApplication {
    public static void main(String[] args) {

        // SpringApplication.run(DatabaseMiniProjectApplication.class, args);
        Long start = System.nanoTime();
        ConfigurableApplicationContext context = SpringApplication.run(DatabaseMiniProjectApplication.class, args);
        MyRepository repository = context.getBean(MyRepository.class);
        String input = readQuery();
        repository.getDataAndStoreIndexes();
        repository.executeJoinwithBitmapIndexes(input);
        Long end = System.nanoTime();
        Long timeElapsed = end-start;
        System.out.println("Total time elpased for the program : " + String.format("%.4f", (float)timeElapsed/1000000) + "(milliseconds)\n\n");
        // repository.executeQuery(input);
    }

    private static String readQuery() {
        System.out.println("\n************************");
        System.out.println("Please enter your query, press N to exit");
        Scanner scanner = new Scanner(System.in);
        StringBuilder input = new StringBuilder();
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            if (line.isEmpty()) {
                break;
            }
            input.append(line).append("\n");
        }
        scanner.close();
        if (input.toString().equals("N")) {
            System.exit(1);
        }
        return input.toString();
    }
}
