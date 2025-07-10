package com.example.sampler;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class StratifiedSampler {

    static class ClaimRecord {
        Map<String, String> attributes;
        int score = 0;
        double probability = 0.0;

        ClaimRecord(String[] headers, String[] values) {
            attributes = new HashMap<>();
            for (int i = 0; i < headers.length; i++) {
                attributes.put(headers[i].trim(), values[i].trim());
            }
        }

        String get(String key) {
            return attributes.getOrDefault(key, "").trim();
        }

        void set(String key, String value) {
            attributes.put(key, value);
        }

        String[] toArray(String[] headers) {
            List<String> row = new ArrayList<>();
            for (String h : headers) {
                row.add(attributes.getOrDefault(h, ""));
            }
            return row.toArray(new String[0]);
        }
    }

    public static void main(String[] args) throws Exception {
        String inputFile = "unsampled-4k.csv";
        String outputFile = "sampled.csv";
        int requiredSamples = 5;

        // Load data
        List<ClaimRecord> records = new ArrayList<>();
        String[] headers;
        try (CSVReader reader = new CSVReader(new FileReader(inputFile))) {
            headers = reader.readNext(); // Read header
            String[] line;
            while ((line = reader.readNext()) != null) {
                records.add(new ClaimRecord(headers, line));
            }
        }

        if (!Arrays.asList(headers).contains("claim_hcc_id")) {
            throw new RuntimeException("❌ 'claim_hcc_id' column not found in data. Check input file headers.");
        }

        // Define weights
        Map<String, Map<String, Integer>> attributeWeights = new HashMap<>();
        String[][] userWeights = {
                {"claim_source", "EDI"}, {"claim_source", "Paper"},
                {"claim_type", "Professional"}, {"claim_type", "Institutional(OP)"}, {"claim_type", "Institutional(IP)"},
                {"status", "Final"}, {"status", "Denied"}, {"status", "Rejected"},
                {"payment_status", "Check Issued"}, {"payment_status", "Check Not Issued"}
        };

        for (String[] w : userWeights) {
            attributeWeights.putIfAbsent(w[0], new HashMap<>());
            attributeWeights.get(w[0]).put(w[1], 100);
        }

        // Calculate score
        for (ClaimRecord r : records) {
            int score = 0;
            for (String attr : attributeWeights.keySet()) {
                String val = r.get(attr);
                score += attributeWeights.get(attr).getOrDefault(val, 0);
            }
            r.score = score;
        }

        // Normalize to probability
        int totalScore = records.stream().mapToInt(r -> r.score).sum();
        if (totalScore > 0) {
            for (ClaimRecord r : records) {
                r.probability = (double) r.score / totalScore;
            }
        }

        // Sampling
        List<ClaimRecord> finalSample;
        if (totalScore > 0 && records.size() >= requiredSamples) {
            finalSample = weightedSample(records, requiredSamples);
        } else {
            // Fallback sampling
            Set<String> usedIds = records.stream()
                    .map(r -> r.get("claim_hcc_id"))
                    .collect(Collectors.toSet());

            List<ClaimRecord> additionalRows = new ArrayList<>();

            for (String[] w : userWeights) {
                for (ClaimRecord r : records) {
                    if (r.get(w[0]).equals(w[1]) && !usedIds.contains(r.get("claim_hcc_id"))) {
                        additionalRows.add(r);
                        usedIds.add(r.get("claim_hcc_id"));
                        break;
                    }
                }
                if (additionalRows.size() >= requiredSamples) break;
            }

            int shortfall = requiredSamples - additionalRows.size();
            Collections.shuffle(records);
            for (ClaimRecord r : records) {
                if (!usedIds.contains(r.get("claim_hcc_id")) && shortfall > 0) {
                    additionalRows.add(r);
                    shortfall--;
                }
            }

            finalSample = additionalRows.stream().limit(requiredSamples).collect(Collectors.toList());
        }

        // Write output
        try (CSVWriter writer = new CSVWriter(new FileWriter(outputFile))) {
            writer.writeNext(headers);
            for (ClaimRecord r : finalSample) {
                writer.writeNext(r.toArray(headers));
            }
        }

        System.out.println(" Sampling complete. Output saved to " + outputFile + ". Total records: " + finalSample.size());
    }

    // Weighted sampling using alias method
    private static List<ClaimRecord> weightedSample(List<ClaimRecord> records, int n) {
        List<ClaimRecord> sample = new ArrayList<>();
        Random random = new Random();

        // Build cumulative distribution
        List<Double> cumulative = new ArrayList<>();
        double total = 0.0;
        for (ClaimRecord r : records) {
            total += r.probability;
            cumulative.add(total);
        }

        // Sample n times
        for (int i = 0; i < n; i++) {
            double rand = random.nextDouble() * total;
            for (int j = 0; j < cumulative.size(); j++) {
                if (rand <= cumulative.get(j)) {
                    sample.add(records.get(j));
                    break;
                }
            }
        }

        return sample;
    }

}
