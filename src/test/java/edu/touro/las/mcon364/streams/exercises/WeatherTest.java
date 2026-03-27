package edu.touro.las.mcon364.streams.exercises;
import edu.touro.las.mcon364.streams.ds.WeatherDataScienceExercise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


public class WeatherTest {

    //----------ParseRow Tests---------------------
    @Test
    void parseRow_wellFormed_withAllData_returnsPopulatedOptional(){
        String row = "S1, Miami, 2026-01-01, 25.5, 80, 0.5";
        // we tell java where to find the record definition. Go to this file, and then to this data structure
        // we call the STATIC method parseRow from the main class == WeatherDataScienceExercise.parseRow(row)
        Optional<WeatherDataScienceExercise.WeatherRecord> result = WeatherDataScienceExercise.parseRow(row);

        assertTrue(result.isPresent());
        assertEquals("S1", result.get().stationId());
        assertEquals("Miami", result.get().city());
        assertEquals("2026-01-01", result.get().date());
        assertEquals(25.5, result.get().temperatureC());
        assertEquals(80, result.get().humidity());
        assertEquals(0.5, result.get().precipitationMm());
    }

    @Test
    void parseRow_missingData_returnsEmptyOptional(){
        String row = "S1, Miami, 2026-01-01, 25.5,";
        Optional<WeatherDataScienceExercise.WeatherRecord> result = WeatherDataScienceExercise.parseRow(row);
        assertFalse(result.isPresent());
    }

    @Test
    void parseRow_missingTemp_returnsEmptyOptional(){
        String row = "S1, Miami, 2026-01-01, , 80, 0.5";
        Optional<WeatherDataScienceExercise.WeatherRecord> result = WeatherDataScienceExercise.parseRow(row);
        assertFalse(result.isPresent());
    }

    @Test
    void parseRow_nonNumericTemp_returnsEmptyOptional(){
        String row = "S1, Miami, 2026-01-01, cold, 80, 0.5";
        Optional<WeatherDataScienceExercise.WeatherRecord> result = WeatherDataScienceExercise.parseRow(row);
        assertFalse(result.isPresent());
    }

    //------------isValid Tests--------------------
    @Test
    void isValid_tempAtMinBoundary_returnsTrue() {
        // since isValid is a static method, it belongs to the class WeatherDataScienceExercise,
        // not to a specific "instance" or object of that class.
        // T4 we DONT do: WeatherDataScienceExercise engine = new WeatherDataScienceExercise();

        // -60.0 is the min and is allowed
        var record = new WeatherDataScienceExercise.WeatherRecord("S1", "City", "Date", -60.0, 50, 0.0);
        assertTrue(WeatherDataScienceExercise.isValid(record));
    }

    @Test
    void isValid_tempJustBelowBoundary_returnsFalse() {
        // -61 is not allowed
        var record = new WeatherDataScienceExercise.WeatherRecord("S1", "City", "Date", -61.0, 50, 0.0);
        assertFalse(WeatherDataScienceExercise.isValid(record));
    }
    @Test
    void isValid_tempAtMaxBoundary_returnsTrue() {
        // 60 is the max
        var record = new WeatherDataScienceExercise.WeatherRecord("S1", "City", "Date", 60.0, 50, 0.0);
        assertTrue(WeatherDataScienceExercise.isValid(record));
    }

    @Test
    void isValid_tempJustAboveUpper_returnsFalse() {
        // 61 is NOT ok
        var record = new WeatherDataScienceExercise.WeatherRecord("S1", "City", "Date", 61.0, 50, 0.0);
        assertFalse(WeatherDataScienceExercise.isValid(record));
    }

    @Test
    void isValid_humidityAtMax_returnsTrue() {
        // 100 humidity is valid
        var record = new WeatherDataScienceExercise.WeatherRecord("S1", "City", "Date", 20.0, 100, 0.0);
        assertTrue(WeatherDataScienceExercise.isValid(record));
    }

    @Test
    void isValid_humidityOverMax_returnsFalse() {
        // 101 humidity is impossible
        var record = new WeatherDataScienceExercise.WeatherRecord("S1", "City", "Date", 20.0, 101, 0.0);
        assertFalse(WeatherDataScienceExercise.isValid(record));
    }

    @Test
    void isValid_humidityAtMin_returnsTrue() {
        // 0 humidity is valid
        var record = new WeatherDataScienceExercise.WeatherRecord("S1", "City", "Date", 20.0, 0, 0.0);
        assertTrue(WeatherDataScienceExercise.isValid(record));
    }

    @Test
    void isValid_humidityUnderMin_returnsFalse() {
        // -1 humidity is impossible
        var record = new WeatherDataScienceExercise.WeatherRecord("S1", "City", "Date", 20.0, -1, 0.0);
        assertFalse(WeatherDataScienceExercise.isValid(record));
    }

    @Test
    void isValid_negative_precipitation_returnsFalse() {
        var record = new WeatherDataScienceExercise.WeatherRecord("S1", "City", "Date", 20.0, 40, -3);
        assertFalse(WeatherDataScienceExercise.isValid(record));
    }

    @Test
    void isValid_precipitation_of_zero_returnsTrue() {
        var record = new WeatherDataScienceExercise.WeatherRecord("S1", "City", "Date", 20.0, 40, 0.0);
        assertTrue(WeatherDataScienceExercise.isValid(record));
    }

    //------ testing the stream stuff :---------

    private List<WeatherDataScienceExercise.WeatherRecord> cleanedData;

    // Helper method specifically for tests to load the real CSV- copied from WeatherDataScienceExercise.java
    private List<String> readCsvForTest(String fileName) throws IOException {
        InputStream in = WeatherDataScienceExercise.class.getResourceAsStream(fileName);
        if (in == null) {
            throw new NoSuchFileException("Resource not found: " + fileName);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        // This runs before every integration test to ensure we have fresh data
        List<String> rawRows = readCsvForTest("noaa_weather_sample_200_rows.csv");

        //first we convert the List <String> into a stream aka conveyor belt. 1 header + 100 rows for example. So streaming all the rows
        List<WeatherDataScienceExercise.WeatherRecord> cleanedData = rawRows.stream()
                .skip(1) // Skip header- bc it's not data
                //takes a string=row and hands it to parseRow method. It returns an Optional<WeatherRecord>.
                // now we have a stream of boxes - some with valid data and some that are empty due to invalid data
                .map(WeatherDataScienceExercise::parseRow)
                // we flatMap and unbox each Optional Box.
                // If the box is full, it opens it and puts the WeatherRecord onto the conveyor belt.
                // If the box is empty, it simply removes it from the belt entirely.
                .flatMap(Optional::stream)
                // now that we have actual objects, we run our business rules. It hands each record to isValid.
                // only the valid objects remain on the belt after the filtering
                .filter(WeatherDataScienceExercise::isValid)
                //our terminal op. Java gathers everything left on the belt into a new List<WeatherRecord>.
                .toList();
    }

    // ---------INTEGRATION TESTS (Using Real CSV)--------------

    @Test
    @DisplayName("Cleaned list is non-empty")
    void testCleanedListIsNotEmpty() {
        // Act: we call the method
        long count = WeatherDataScienceExercise.countValidRecords(cleanedData);

        // Assert: we expect more than 0 records from the 200-row file
        assertTrue(count > 0, "The count of valid records should be greater than zero");
    }

    @Test
    @DisplayName("All records in cleaned list pass isValid")
    void testAllRecordsPassIsValid() {
        // Every single record that made it to the list must be 'valid'
        // The allMatch method is a terminal operation in the Java Stream API.
        // it looks at every single item on the conveyor belt and asks one question: Do ALL of you satisfy this rule?
        // if even one single item fails the test, the entire method immediately returns false
        assertTrue(cleanedData.stream().allMatch(WeatherDataScienceExercise::isValid),
                "Found a record in the cleaned list that does not meet validation rules!");
    }

    @Test
    @DisplayName("Highest avg temp city is a valid string aka its NOT null and its NOT empty")
    void integration_highestAvgTempCityIsValidString() {
    // Act: Call the static method we just created in the main class
    // We pass in 'cleanedData' which was populated in the @BeforeEach setUp()
    String highestCity = WeatherDataScienceExercise.findCityWithHighestAvgTemp(cleanedData);

    // Assert: Verify the result is what we expect from a real dataset
    assertNotNull(highestCity);
    assertFalse(highestCity.trim().isEmpty());
    assertNotEquals("N/A", highestCity, "The list was not empty, so it shouldn't return N/A");
    }

    @Test
    @DisplayName("Wettest day has precipitation >= 0")
    void testWettestDayPrecipitationValue() {
        // Act: we need to call the refactored method
        Optional<WeatherDataScienceExercise.WeatherRecord> wettest = WeatherDataScienceExercise.findWettestDay(cleanedData);

        // Assert: we verify the result - if there's a record in the list -> true. if not -> it wld fail the test
        assertTrue(wettest.isPresent());

        // in Java testing, assertions run in order. If the first one fails, the test stops immediately.
        // Since the test starts with assertTrue(wettest.isPresent()), that acts as a safety gate.
        // so if there is an empty list, it will never reach the code below in this test.

        double precip = wettest.get().precipitationMm();
        assertTrue(precip >= 0, "Precipitation value " + precip + " should not be negative");
    }

}

