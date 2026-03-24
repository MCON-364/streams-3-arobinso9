package edu.touro.las.mcon364.test;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class MidtermAutograderTest {

    @Test
    void problem01_currentMonthSupplier_returnsCurrentMonth() {
        Supplier<Integer> supplier = FunctionalWarmup.currentMonthSupplier();
        assertEquals(LocalDate.now().getMonthValue(), supplier.get());
    }

    @Test
    void problem02_longerThanFive_worksForShortAndLongStrings() {
        Predicate<String> predicate = FunctionalWarmup.longerThanFive();
        assertFalse(predicate.test("Java"));
        assertTrue(predicate.test("Streams"));
    }

    @Test
    void problem03_positiveAndEven_usesExpectedLogic() {
        Predicate<Integer> predicate = FunctionalWarmup.positiveAndEven();
        assertTrue(predicate.test(8));
        assertFalse(predicate.test(7));
        assertFalse(predicate.test(-4));
    }

    @Test
    void problem04_wordCounter_countsWordsAndHandlesBlank() {
        Function<String, Integer> function = FunctionalWarmup.wordCounter();
        assertEquals(4, function.apply("  one two   three four  "));
        assertEquals(0, function.apply("   "));
    }

    @Test
    void problem05_cleanLabels_filtersTrimsAndUppercases() {
        List<String> result = FunctionalWarmup.cleanLabels(List.of("  math ", "", " java", "  ", "oop"));
        assertEquals(List.of("MATH", "JAVA", "OOP"), result);
    }

    @Test
    void problem06_getSortedCourseNames_returnsAlphabeticalNames() {
        BasicStreamsQuiz quiz = new BasicStreamsQuiz();
        assertEquals(List.of("Algorithms", "Databases", "Java", "Networks"), quiz.getSortedCourseNames());
    }

    @Test
    void problem07_countScoresAtLeast_countsAcrossAllCourses() {
        BasicStreamsQuiz quiz = new BasicStreamsQuiz();
        assertEquals(6, quiz.countScoresAtLeast(90));
        assertEquals(10, quiz.countScoresAtLeast(80));
    }

    @Test
    void problem08_firstLongWord_returnsOptionalResult() {
        BasicStreamsQuiz quiz = new BasicStreamsQuiz();
        assertEquals(Optional.of("inheritance"), quiz.firstLongWord(List.of("oop", "inheritance", "map"), 5));
        assertEquals(Optional.empty(), quiz.firstLongWord(List.of("cat", "dog"), 5));
    }

    @Test
    void problem09_squareAll_returnsSquaredValues() {
        BasicStreamsQuiz quiz = new BasicStreamsQuiz();
        assertEquals(List.of(1, 4, 9, 16), quiz.squareAll(List.of(1, 2, 3, 4)));
    }

    @Test
    void problem10_averagePassingScore_usesOnlyPassingValues() {
        BasicStreamsQuiz quiz = new BasicStreamsQuiz();
        assertEquals(84.9333, quiz.averagePassingScore(), 0.0001);
    }

    @Test
    void problem11_addLearner_addsNewLearnerAndRejectsDuplicate() {
        StudyTracker tracker = new StudyTracker();
        assertTrue(tracker.addLearner("Mia"));
        assertFalse(tracker.addLearner("Mia"));
        assertTrue(tracker.learnerNames().contains("Mia"));
    }

    @Test
    void problem12_addScore_addsOnlyForExistingLearner() {
        StudyTracker tracker = new StudyTracker();
        tracker.addLearner("Noah");
        assertTrue(tracker.addScore("Noah", 88));
        assertEquals(List.of(88), tracker.scoresFor("Noah").orElseThrow());
        assertFalse(tracker.addScore("Ava", 91));
    }

    @Test
    void problem13_averageFor_returnsOptionalAverage() {
        StudyTracker tracker = new StudyTracker();
        tracker.addLearner("Liam");
        tracker.addScore("Liam", 80);
        tracker.addScore("Liam", 100);
        assertEquals(Optional.of(90.0), tracker.averageFor("Liam"));
        assertEquals(Optional.empty(), tracker.averageFor("Unknown"));
    }

    @Test
    void problem14_letterBandFor_mapsAverageToLetter() {
        StudyTracker tracker = new StudyTracker();
        tracker.addLearner("Emma");
        tracker.addScore("Emma", 82);
        tracker.addScore("Emma", 88);
        assertEquals(Optional.of("B"), tracker.letterBandFor("Emma"));
    }

    @Test
    void problem15_undoLastChange_revertsMostRecentMutation() {
        StudyTracker tracker = new StudyTracker();
        tracker.addLearner("Zoe");
        tracker.addScore("Zoe", 91);
        assertTrue(tracker.undoLastChange());
        assertEquals(List.of(), tracker.scoresFor("Zoe").orElseThrow());

        assertFalse(tracker.undoLastChange());
        assertTrue(tracker.scoresFor("Zoe").isPresent());
    }
}
