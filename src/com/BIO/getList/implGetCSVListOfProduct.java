package com.BIO.getList;

import com.BIO.POJO.Product;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.BIO.Main.*;


public class implGetCSVListOfProduct implements iGetCSVListOfProduct {

    private static final String COMMA = ",";
    private long linesCounter = 0;

    public implGetCSVListOfProduct(String inputFilePath) {
        productsList = processInputCSVFilteredByIDAndCount(inputFilePath);
    }

    private List<Product> productsList;

    // treemap < price , Product >
    Map<Double, Product> rep1 = new TreeMap<>();

    public List<Product> processInputCSVFilteredByIDAndCount(String inputFilePath) {
        try {
            // read each file in a separate thread
            File inFile = new File(inputFilePath);
            InputStream inFS = new FileInputStream(inFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(inFS));

            String readLine = "";
            // ID, Name, Condition, State, Price
            String header = br.readLine();

            // map  ID <--> counter
            HashMap<Integer, Integer> ids = new HashMap<>();

            productsList = new ArrayList<>();

            int linesCounter = 0;

            while ((readLine = br.readLine()) != null) {
                linesCounter++;

                // read each row, split, parse to Product class
                String[] elements = readLine.split(COMMA);

                try {
                    Product product = new Product();
                    int id = Integer.parseInt(elements[0]);
                    product.setID(id);
                    product.setName(elements[1].trim());
                    product.setCondition(elements[2].trim());
                    product.setState(elements[3].trim());
                    double price = Double.parseDouble(elements[4].trim());
                    product.setPrice(price);

                    // save Product
                    rep1.put(price, product);

                    // sort by price and remove identical id >20
                    // shrink unnecessary lines /sort/trim every 5000 lines
                    if (linesCounter > REPORT_LINES_MAX * KOEFF) {
                        linesCounter = 0;

                        int val = rep1.size();
                        shrink();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            br.close();

            // sort by price// shrink rep1
            shrink();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return productsList.stream().limit(REPORT_LINES_MAX).collect(Collectors.toList());
    }


    HashMap<Integer, Integer> analyzeIDfreq(List<Product> listProducts) {
        // id - count
        HashMap<Integer, Integer> hashMap = new HashMap<>();
        listProducts.stream().forEach(product -> {
            int id = product.getID();
            // freq ID analysis
            int idCounter = hashMap.get(id) == null ? 0 : hashMap.get(id);
            hashMap.put(id, idCounter + 1);
        });

        return hashMap;
    }

    HashMap<Integer, Integer> hmBadIds(HashMap<Integer, Integer> hmAnalyzed) {
        HashMap<Integer, Integer> hmBadIds = new HashMap<>();
        for (int _id : hmAnalyzed.keySet()) {
            int freq = hmAnalyzed.get(_id);
            // save bad id
            if (freq > REPORT_SIMILAR_ID_MAX) hmBadIds.put(_id, freq);
        }
        return hmBadIds;
    }

    synchronized void shrink() {
        productsList = rep1.values().stream().collect(Collectors.toList());
        HashMap<Integer, Integer> hmIdFreq = analyzeIDfreq(productsList);
        HashMap<Integer, Integer> hmBadIds = hmBadIds(hmIdFreq);

        for (int id : hmBadIds.keySet()) { // iterate bad IDs
            // for each id > 20 I get the list of Products, filter by bad ID, list is sorted,
            // skip first 20 elements and convert to list again
            List<Product> productsListBadID = productsList.stream()
                    .filter(product -> product.getID() == id)
                    .skip(REPORT_SIMILAR_ID_MAX)
                    .collect(Collectors.toList());

            // remove from report bad ids
            productsListBadID.stream().forEach(product -> rep1.remove(product.getPrice(), product));
        }
    }

    @Override
    public List<Product> getProductsList() {
        return productsList;
    }

}
