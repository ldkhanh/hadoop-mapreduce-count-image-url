package com.wm.mapreduce;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * created Khanh
 * at Dec 10 2019
 */
public class SummaryOutput {
    public void run(String path) {
        String folderPath = path;
        String outputPath = path + ((path.charAt(path.length()-1) == '/')? "" : "/") + "summary.txt";
        Map<Integer, Integer> map = new HashMap<>();
        try {
            final File folder = new File(folderPath);
            for (final File file : folder.listFiles()) {
                if (!file.isDirectory()) {
                    if (file.getName().startsWith("part")) {
                        readFile(file.getPath(), map);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        writeOutput(outputPath, map);
    }

    private void readFile(final String filePath, Map<Integer, Integer> map) {

        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            stream.forEach(
                    e -> {
                        //System.out.println(e);
                        String[] st = e.split("\\s+");
                        int count = Integer.parseInt(st[1]);
                        map.put(count, map.getOrDefault(count,0) + 1);
                    }
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeOutput(final String outputFile, Map<Integer, Integer> map) {
        System.out.println(map);
        int total = 0;
        StringBuilder sb = new StringBuilder();
        sb.append("FrequencyRead     |     TotalNumber").append("\n");
        for (Map.Entry<Integer, Integer> kv : map.entrySet()) {
            total += kv.getKey() * kv.getValue();
            sb.append(kv.getKey()).append("\t\t").append(kv.getValue()).append("\n");
        }
        sb.append("Total number URL: ").append(total);

        File file = new File(outputFile);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
