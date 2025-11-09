package org.example.crossCorrelation;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class OrderGenerator {
    public static void main(String[] args) throws Exception {
        int numOrders = 1000;
        int numProducts = 100;
        Random r = new Random();
        List<String> products = new ArrayList<>();
        for (int i = 1; i <= numProducts; i++) {
            products.add(String.format("product_%03d", i));
        }

        try (PrintWriter pw = new PrintWriter("orders.txt")) {
            for (int i = 0; i < numOrders; i++) {
                int numItems = r.nextInt(8) + 1; // 1–8
                Collections.shuffle(products);
                List<String> order = products.subList(0, numItems);
                pw.println(String.join(" ", order));
            }
        }
    }
}