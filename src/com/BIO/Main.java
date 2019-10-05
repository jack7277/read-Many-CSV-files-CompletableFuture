package com.BIO;

import com.BIO.POJO.Product;
import com.BIO.getList.iGetCSVListOfProduct;
import com.BIO.getList.implGetCSVListOfProduct;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {
    public static int REPORT_LINES_MAX = 1000; // Number of goods in a report by default
    public static int REPORT_SIMILAR_ID_MAX = 20; // Number of goods with similar ID by default
    public static int KOEFF = 5;

    // output report
    private static Map<Double, Product> fReportHM;

    public static void main(String[] args) {
        // global final report to output, sorted by price
        fReportHM = Collections.synchronizedMap(new TreeMap<>());

        if (args.length == 2) {
            try {
                REPORT_LINES_MAX = Integer.parseInt(args[0].toString());
                REPORT_SIMILAR_ID_MAX = Integer.parseInt((args[1].toString()));
            } catch (NumberFormatException e) {
                System.out.println("Error, please, use digits in arguments via space.");
                System.out.println("No argument, using default settings: Number of goods = " +
                        REPORT_LINES_MAX +
                        ", Number of similar IDs: " +
                        REPORT_SIMILAR_ID_MAX);
            }
        } else {
            System.out.println("No arguments, using default settings: Number of goods = " +
                    REPORT_LINES_MAX +
                    ", Number of similar IDs = " +
                    REPORT_SIMILAR_ID_MAX);
        }

        // get threads limit -1 or system may freeze on large files
        int threadsLimit = Runtime.getRuntime().availableProcessors();
        threadsLimit = Math.max(threadsLimit, 1);

        String timeStamp = new SimpleDateFormat("dd.MM.yyyy HH.mm.ss").format(new Date());
        System.out.println(timeStamp);

        System.out.println("Number of threads = " + threadsLimit);

        // get the list of files from input folder
        File[] filesList = new File("./files_in/").listFiles();

        // run in a fixed thread pool
        ExecutorService executorService = Executors.newFixedThreadPool(threadsLimit);

        // start the timer
        long startTime = System.nanoTime();

        // for each file from input folder I get the file and send it as a thread
        // thread reads file partially and sort/filter, return map<file, productList>
        for (File file : filesList) {
            CompletableFuture<HashMap<File, List<Product>>> cf = CompletableFuture
                    .supplyAsync(() -> {// async task with get result
                        // start buffered reader, sort, filter
                        iGetCSVListOfProduct csvReader = new implGetCSVListOfProduct(file.getPath());
                        List<Product> productsList = csvReader.getProductsList();

                        HashMap<File, List<Product>> obj = new HashMap<>();
                        obj.put(file, productsList);
                        return obj;// send when thread finish
                    }, executorService);

            cf.thenAccept(hmFileListOfProduct -> {
                if (hmFileListOfProduct == null) return;

                // extract file and filename from finished thread
                Map.Entry<File, List<Product>> entry = hmFileListOfProduct.entrySet().iterator().next();
                File _file = entry.getKey();
                String fname = file.getPath();

                // show info about finished task
                System.out.println(hmFileListOfProduct.get(_file).size() + " filtered lines from file: " + fname);

                // extract List <Product>
                List<Product> lp = hmFileListOfProduct.get(_file);

                // save filtered info from each file to Global report
                lp.stream().forEach(product -> fReportHM.put(product.getPrice(), product));

                // shrink ?
                // stupid save result each thread
                // I moved this to the end, one time save and shrink
            }).handle((object, error) -> {
                System.out.println(error.getMessage());
                return -1;
            });
        }

        executorService.shutdown();
        try {
            // wait all finished
            executorService.awaitTermination(1, TimeUnit.DAYS);

            // shrink report to 1000 lines
            Shrink shrink = new Shrink();

            // save to output.txt
            List<Product> lp2 = fReportHM.values().stream().collect(Collectors.toList());
            saveFile(lp2);

            // show duration of total process
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000000L;
            System.out.println("All files done in, sec: " + duration);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static void saveFile(List<Product> lp2) {
        Path path = Paths.get("output.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write("ID,Name,Condition,State,Price\n");
            lp2.stream().limit(REPORT_LINES_MAX).forEach(product -> {
                try {
                    writer.write(product.getID() +
                            "," + product.getName() +
                            "," + product.getCondition() +
                            "," + product.getState() +
                            "," + product.getPrice() + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class Shrink {
        Shrink() {
            shrink();
        }

        // trim to 1000 lines
        synchronized void shrink() {
            List<Product> productsList = fReportHM.values().stream().collect(Collectors.toList());
            HashMap<Integer, Integer> hmIdFreq = analyzeIDfreq(productsList);
            HashMap<Integer, Integer> hmBadIds = hmBadIds(hmIdFreq); // id - count

            for (int id : hmBadIds.keySet()) { // iterate bad IDs
                List<Product> productsListBadID = productsList.stream()
                        .filter(product -> product.getID() == id)
                        .skip(REPORT_SIMILAR_ID_MAX)
                        .collect(Collectors.toList());

                // remove bad from report
                productsListBadID.stream().forEach(product ->
                        fReportHM.remove(product.getPrice(), product));
            }
        }

        // id-count
        synchronized HashMap<Integer, Integer> analyzeIDfreq(List<Product> listProducts) {
            HashMap<Integer, Integer> hashMap = new HashMap<>();
            listProducts.stream().forEach(product1 -> {
                int id = product1.getID();
                // freq ID analysis
                int idCounter = hashMap.get(id) == null ? 0 : hashMap.get(id);
                hashMap.put(id, idCounter + 1);
            });

            return hashMap;
        }

        // id-count
        synchronized HashMap<Integer, Integer> hmBadIds(HashMap<Integer, Integer> hmAnalyzed) {
            HashMap<Integer, Integer> hmBadIds = new HashMap<>();
            for (int _id : hmAnalyzed.keySet()) {
                int freq = hmAnalyzed.get(_id);
                if (freq > REPORT_SIMILAR_ID_MAX) hmBadIds.put(_id, freq);
            }
            return hmBadIds;
        }
    }
}
