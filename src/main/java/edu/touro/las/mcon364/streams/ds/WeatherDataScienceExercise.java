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
        Map <String, Double> city_to_avg_precipitation= cleaned.stream()
                .collect(Collectors.groupingBy(
                        WeatherRecord::city, // city will be the key
                        //downstream collector:
                        //When you provide a second argument to groupingBy, you are telling the Stream:
                        // Don't just give me a list of records for this city. Instead, take all the
                        // records for this city and reduce them into a single number using this formula.
                        Collectors.averagingDouble(WeatherRecord::precipitationMm)));

        // TODO 6:
        // Partition rows into freezing days (temperature <= 0)
        // and non-freezing days (temperature > 0).
        Map<Boolean, List<WeatherRecord>> partitionedByFreezing = cleaned.stream()
                .collect(Collectors.partitioningBy(r -> r.temperatureC() <= 0));

        /* If we wanted to have the citys as values instead of by records, use downstream collector
        Collectors.mapping(                   // 2. The Downstream Collector
                        WeatherRecord::city,          // 3. What to extract
                        Collectors.toList()           // 4. How to store it
                )
         */

        // TODO 7:
        // Create a Set<String> of all distinct cities.
        Set <String> distinct_cities =  cleaned.stream()
                //transform to a stream of cities
                .map(WeatherRecord::city)
                .collect(Collectors.toSet());

        // TODO 8:
        // Find the wettest single day.
        Optional<WeatherRecord> wettestDay = cleaned.stream()
                .max(Comparator.comparingDouble(WeatherRecord::precipitationMm));
        // To get a String for output:
        String result = wettestDay
                .map(r -> r.date() + " in " + r.city() + " with " + r.precipitationMm() + "mm")
                .orElse("No data available");

        /* To do TODO 8 with reduce:
        Optional<WeatherRecord> wettestDay = cleaned.stream()
        .reduce((record1, record2) ->
            record1.precipitationMm() >= record2.precipitationMm() ? record1 : record2
        );

         */

        // TODO 9:
        // Create a Map<String, Double> from city to average humidity.
        Map<String, Double> city_to_avg_humidity =
                cleaned.stream()
                        .collect(Collectors.groupingBy(
                                WeatherRecord::city,
                                Collectors.averagingDouble(WeatherRecord::humidity)));

        // TODO 10:
        /* .forEach() is used for actions (like printing to the console).
        However, when you want to create a new collection, .map() is the right tool bc
        it keeps the conveyor belt moving toward a final list.
        1- Change one thing into another ->	.map() -> A new Stream of the new type
        2- Perform an action (Print/Save) -> .forEach() -> Nothing (Void)
        */
        // Produce a list of formatted strings like:
        // "Miami on 2025-01-02: 25.1C, humidity 82%"
        // %f -> float or double
        // %d -> integer
        // %c -> char
        // %s -> string
        List <String> formatted_strings =
                cleaned.stream()
                        .map(r -> String.format("%s on %s: %.1fC, humidity %d%%",
                                r.city(),
                                r.date(),
                                r.temperatureC(),
                                r.humidity()))
                        .toList();

        // TODO 11 (optional):
        // I used AI for this- I could not do 11 by myself.
        // Build a Map<String, CityWeatherSummary> for all cities.
        // Put your code below these comments or refactor into helper methods.
        // summaryStatistics() automatically does these methods when u call it on ur stream:
        // stats.getCount(), stats.getAverage(), stats.getMax(), stats.getMin(), stats.getSum()
        //It returns a DoubleSummaryStatistics object that has already calculated
        // the Count, Sum, Min, Max, and Average for you.
        Map<String, CityWeatherSummary> city_to_summary = cleaned.stream()
                .collect(Collectors.groupingBy(
                        WeatherRecord::city, // Step 1: Decide the "Folder" name (City)
                        Collectors.collectingAndThen( // Step 2: "Collect, then transform"
                                Collectors.toList(), // 2a: Put all records for that city in a list
                                list -> {            // 2b: Use that list to build the Summary object

                                    // we loop through each unique Key's list twice
                                    // goes through a unique city list once to get the Count, Average Temp, and Max Temp.
                                    // This single line finds Average, Max, and Count for Temp
                                    DoubleSummaryStatistics tempStats = list.stream()
                                            .mapToDouble(WeatherRecord::temperatureC)
                                            .summaryStatistics();

                                    // goes through the same unique city list a second time to calculate avg precipitation
                                    // This handles the Rainfall average separately
                                    double avgPrecip = list.stream()
                                            .mapToDouble(WeatherRecord::precipitationMm)
                                            .average().orElse(0.0);

                                    // Now we plug those numbers into your Constructor
                                    return new CityWeatherSummary(
                                            list.get(0).city(),    // cityName
                                            tempStats.getCount(),  // dayCount
                                            tempStats.getAverage(),// avgTemp
                                            avgPrecip,             // avgPrecipitation
                                            tempStats.getMax()     // maxTemp
                                    );
                                }
                        )
                ));

    }
    /* in a parseRow method, we usually focus on two specific things to decide if a row is malformed:
    Length and Data Quality.
    If a row is supposed to have 6 columns but only has 4, it's malformed.
    If the temperature column contains Banana instead of 25.0, it's malformed.

     */
    static Optional<WeatherRecord> parseRow(String row) {
        // TODO:
        // 1. Split the row by commas
        String[] parts = row.split(",");

        // 2. Reject malformed rows - WeatherRecord has 6 fields
        if (parts.length < 6) {
            return Optional.empty(); // Not enough data to make a record
        }

        // 3. Reject rows with missing temperature
        if (parts[3] == null)
            return Optional.empty();

        // 3.1 Reject rows with any missing data
        /*
        for (String word : parts) {
            if (word == null || word.trim().isEmpty()) {
            return Optional.empty(); // Reject if any "cell" is blank }}
         */

        // 4. Parse numeric values safely
        try {
            String stationId = parts[0].trim();
            String city = parts[1].trim();
            String date = parts[2].trim();

            // Convert the "Math" fields
            double temp = Double.parseDouble(parts[3].trim());
            int humidity = Integer.parseInt(parts[4].trim());
            double precip = Double.parseDouble(parts[5].trim());

            // 5. Return Optional.empty() if parsing fails
            return Optional.of(new WeatherRecord(stationId, city, date, temp, humidity, precip));

        } catch (NumberFormatException e) {
            // If temperature is "N/A" or humidity is "Low", catch it here
            return Optional.empty();
        }
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
