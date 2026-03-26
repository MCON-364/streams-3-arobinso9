package edu.touro.las.mcon364.streams.ds;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.stream.*;


public class WeatherDataScienceExercise {

    record WeatherRecord(
            String stationId,
            String city,
            String date,
            double temperatureC,
            int humidity,
            double precipitationMm
    ) {}

    public static void main(String[] args) throws Exception {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");

        List<WeatherRecord> cleaned = rows.stream()
                .skip(1) // skip header
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();

        System.out.println("Total raw rows (excluding header): " + (rows.size() - 1));
        System.out.println("Total cleaned rows: " + cleaned.size());

        // TODO 1:
        // Count how many valid weather records remain after cleaning.
        int validCount= cleaned.size();

        // TODO 2:
        // Compute the average temperature across all valid rows.
        // When we call .orElse(0.0), we are telling Java: Try to give me the avg,
        // but if the stream was empty and no average exists, just give me 0.0 instead.
        //This unwraps the value from the OptionalDouble container and turns it into a
        // standard primitive double.
        double avg= cleaned.stream()
                .mapToDouble(weather-> weather.temperatureC) // or WeatherRecord::temperatureC
                .average()
                .orElse(0);

        // TODO 3:
        // Find the city with the highest average temperature.
        String city_highest_avg= cleaned.stream()
                //first we make a map of each city with their avg temp as the value
                .collect(Collectors.groupingBy(
                        WeatherRecord::city,
                        Collectors.averagingDouble(WeatherRecord::temperatureC)))
                // A Map itself is not a stream. To process it, we call .entrySet(),
                // which turns the Map into a Set of Entry objects - Key, Value pairs, then we stream it
                .entrySet().stream()
                //take an entry and compare it by its value- which is the avg temp
                // and then .max looks thru all objs to find one with highest temp value
                .max(Map.Entry.comparingByValue())
                //then we get the key- the nam eof the city with the highest temp
                .map(Map.Entry::getKey)
                //if the og list was empty, return NA to prevent crashes
                .orElse("N/A");

        // TODO 4:
        // Group records by city.
        //.collect is terminal op
        // When you don't provide a second downstream collector, Java assumes you want to keep
        // all the original objects and just sort them into buckets.
        // so here the value would be WeatherRecord bc we streamed a list of them
        Map<String, List<WeatherRecord>> byCity= cleaned.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city));


        // TODO 5:
        // Compute average precipitation by city.

        // TODO 6:
        // Partition rows into freezing days (temperature <= 0)
        // and non-freezing days (temperature > 0).

        // TODO 7:
        // Create a Set<String> of all distinct cities.

        // TODO 8:
        // Find the wettest single day.

        // TODO 9:
        // Create a Map<String, Double> from city to average humidity.

        // TODO 10:
        // Produce a list of formatted strings like:
        // "Miami on 2025-01-02: 25.1C, humidity 82%"

        // TODO 11 (optional):
        // Build a Map<String, CityWeatherSummary> for all cities.

        // Put your code below these comments or refactor into helper methods.
    }

    static Optional<WeatherRecord> parseRow(String row) {
        // TODO:
        // 1. Split the row by commas
        // 2. Reject malformed rows
        // 3. Reject rows with missing temperature
        // 4. Parse numeric values safely
        // 5. Return Optional.empty() if parsing fails

        throw new UnsupportedOperationException("TODO: implement parseRow");
    }

    static boolean isValid(WeatherRecord r) {
        // TODO:
        // Keep only rows where:
        // - temperature is between -60 and 60
        // - humidity is between 0 and 100
        // - precipitation is >= 0

        throw new UnsupportedOperationException("TODO: implement isValid");
    }

    record CityWeatherSummary(
            String city,
            long dayCount,
            double avgTemp,
            double avgPrecipitation,
            double maxTemp
    ) {}

    private static List<String> readCsvRows(String fileName) throws IOException {
        InputStream in = WeatherDataScienceExercise.class.getResourceAsStream(fileName);
        if (in == null) {
            throw new NoSuchFileException("Classpath resource not found: " + fileName);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }
}
