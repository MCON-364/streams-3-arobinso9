package edu.touro.las.mcon364.streams.demo;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DemoReduce {

    // =========================================================
    // Records
    // =========================================================

    record Student(String name, String major, double gpa, List<String> courses) {}

    record DepartmentReport(Map<String, Integer> majorCounts, double avgGpa) {}

    record MajorStats(String major, long studentCount, double avgGpa, Set<String> uniqueCourses) {}

    // Used internally for example 2
    record DepartmentAccumulator(Map<String, Integer> majorCounts, double totalGpa, int count) {
        DepartmentReport toReport() {
            double avg = count == 0 ? 0.0 : totalGpa / count;
            return new DepartmentReport(Map.copyOf(majorCounts), avg);
        }
    }

    // Mutable box used by Collector.of(...) for example 4
    static class MajorStatsAccumulator {
        private final String major;
        private long studentCount = 0;
        private double totalGpa = 0.0;
        private final Set<String> uniqueCourses = new HashSet<>();

        MajorStatsAccumulator(String major) {
            this.major = major;
        }

        void add(Student s) {
            studentCount++;
            totalGpa += s.gpa();
            uniqueCourses.addAll(s.courses());
        }

        MajorStatsAccumulator combine(MajorStatsAccumulator other) {
            this.studentCount += other.studentCount;
            this.totalGpa += other.totalGpa;
            this.uniqueCourses.addAll(other.uniqueCourses);
            return this;
        }

        MajorStats toStats() {
            double avg = studentCount == 0 ? 0.0 : totalGpa / studentCount;
            return new MajorStats(major, studentCount, avg, Set.copyOf(uniqueCourses));
        }
    }

    // =========================================================
    // example 1
    // =========================================================

    /**
     * Find the student with the highest GPA using reduce.
     * @param students - list of students to search through
     * @return - an Optional containing the student with the highest GPA, or Optional.empty() if the list is empty
     */
    static Optional<Student> highestGpaWithReduce(List<Student> students) {
        return students.stream()
                .reduce((s1, s2) -> s1.gpa() >= s2.gpa() ? s1 : s2);
    }

    /**
     * Join all student names into a single string separated by " | ".
     * @param students - list of students whose names to join
     * @return - a string of all student names separated by " | ", or an empty string if the list is empty
     */
    static String joinNamesWithReduce(List<Student> students) {
        return students.stream()
                .map(Student::name)
                .reduce((a, b) -> a + " | " + b)
                .orElse("");
    }

    /**
     * Count the total number of unique courses taken by all students using the three-arg form of reduce.
     * @param students - list of students whose courses to count
     * @return - the count of unique courses across all students
     */
    static int countUniqueCoursesWithThreeArgReduce(List<Student> students) {
        Set<String> uniqueCourses = students.stream()
                .reduce(
                        //identity: the result of reducing an empty stream should be an empty set of courses
                        new HashSet<>(), 
                        // accumulator: add all courses from the current student to the set
                        // Note that we are passing the mutable set as a first argument to the accumulator,
                        // which is a common pattern for reduce when you want to mutate an accumulator object.
                        // We must return the set from the accumulator, even though we're mutating it
                        //because that's how the three-arg reduce works
                        (set, student) -> { 
                            set.addAll(student.courses());         
                            return set;                            
                        },
                        // combiner: combine two sets of courses by adding all from the right into the left
                        (left, right) -> {
                            left.addAll(right);
                            return left;
                        }
                );

        return uniqueCourses.size();
    }
    static void DemoExample1(List<Student> students) {
        Optional<Student> highestGpaStudent = highestGpaWithReduce(students);
        String joinedNames = joinNamesWithReduce(students);
        int uniqueCourseCount = countUniqueCoursesWithThreeArgReduce(students);

        System.out.println("Highest GPA student:");
        System.out.println(highestGpaStudent.orElse(null));

        System.out.println("\nAll names joined:");
        System.out.println(joinedNames);

        System.out.println("\nTotal unique courses across all students:");
        System.out.println(uniqueCourseCount);
    }
    // =========================================================
    // example 2
    // =========================================================

    /**
     * Build a DepartmentReport containing:
     * - a map of major → count of students in that major
     * - the average GPA across all students
     *
     * @param stream - a stream of students to analyze
     * @return - a DepartmentReport with the major counts and average GPA
     */
    static DepartmentReport buildDepartmentReport(java.util.stream.Stream<Student> stream) {
        DepartmentAccumulator acc = stream.reduce(
                // identity: start with an empty major count map, total GPA of 0, and count of 0
                new DepartmentAccumulator(new HashMap<>(), 0.0, 0),
                // accumulator: for each student, update the major count and add to total GPA and count
                (current, student) -> {
                    Map<String, Integer> nextCounts = new HashMap<>(current.majorCounts());
                    nextCounts.merge(student.major(), 1, Integer::sum);
                    return new DepartmentAccumulator(
                            nextCounts,
                            current.totalGpa() + student.gpa(),
                            current.count() + 1
                    );
                },
                // combiner: combine two accumulators by merging their major counts and summing their GPAs and counts
                (left, right) -> {
                    Map<String, Integer> mergedCounts = new HashMap<>(left.majorCounts());
                    right.majorCounts().forEach((major, count) -> mergedCounts.merge(major, count, Integer::sum));
                    return new DepartmentAccumulator(
                            mergedCounts,
                            left.totalGpa() + right.totalGpa(),
                            left.count() + right.count()
                    );
                }
        );

        return acc.toReport();
    }
    static void DemoExample2(List<Student> students) {
        DepartmentReport sequential = buildDepartmentReport(students.stream());
        DepartmentReport parallel = buildDepartmentReport(students.parallelStream());

        System.out.println("Sequential report:");
        System.out.println(sequential);

        System.out.println("\nParallel report:");
        System.out.println(parallel);

        System.out.println("\nReports equal? " + sequential.equals(parallel));
    }
    // =========================================================
    // example 3
    // =========================================================
    static int brokenParallelSum(List<Integer> numbers) {
        int[] shared = new int[1]; // deliberately unsafe shared mutable state

        numbers.parallelStream()
                .map(n -> {
                    shared[0] += n;   // race condition
                    return n;
                })
                .toList();

        return shared[0];
    }

    static long correctParallelSum(List<Integer> numbers) {
        return numbers.parallelStream()
                .mapToLong(Integer::longValue)
                .sum();
    }
    static void DemoExample3() {
        List<Integer> numbers = IntStream.rangeClosed(1, 100_000).boxed().toList();

        long expectedSum = numbers.stream().mapToLong(Integer::longValue).sum();
        System.out.println("Expected sum = " + expectedSum);

        // Broken example: shared mutable state
        Set<Integer> brokenResults = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            brokenResults.add(brokenParallelSum(numbers));
        }

        System.out.println("\nBroken parallel results across 100 runs:");
        System.out.println("Distinct results count = " + brokenResults.size());
        System.out.println("Some results = " + brokenResults.stream().limit(10).toList());

        // Correct example
        Set<Long> fixedResults = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            fixedResults.add(correctParallelSum(numbers));
        }

        System.out.println("\nFixed parallel results across 100 runs:");
        System.out.println("Distinct results count = " + fixedResults.size());
        System.out.println("Result = " + fixedResults);
    }



    // =========================================================
    // example 4
    // =========================================================

    static void DemoExample4(List<Student> students) {
//        Map<String, MajorStats> sequential = buildMajorStatsByMajor(students.stream());
//        Map<String, MajorStats> parallel = buildMajorStatsByMajor(students.parallelStream());

        Map<String, MajorStats> sequentialFixed = buildMajorStatsByMajorFixed(students.stream());
        Map<String, MajorStats> parallelFixed = buildMajorStatsByMajorFixed(students.parallelStream());
        System.out.println("Sequential major stats:");
        printMajorStatsMap(sequentialFixed);

        System.out.println("\nParallel major stats:");
        printMajorStatsMap(parallelFixed);

        System.out.println("\nOutputs equal? " + sequentialFixed.equals(parallelFixed));
    }

    /**
     * Build a map of major → MajorStats containing:
     * - the major name
     * - the count of students in that major
     * - the average GPA for that major
     * - the set of unique courses taken by students in that major
     * @param stream
     * @return
     */
    static Map<String, MajorStats> buildMajorStatsByMajor(java.util.stream.Stream<Student> stream) {
        return stream.collect(
                Collectors.groupingBy(
                        Student::major,
                        Collector.of(
                                // supplier: create a new accumulator with the major set to null (we will inject the major later since it's not available at this level)
                                () -> new MajorStatsAccumulator(null),
                                // accumulator: add the student to the accumulator, but we can't set the major here since it's not available in this context
                                (acc, student) -> {
                                    if (acc.major == null) {
                                        // not possible to reassign final field, so use a different supplier strategy below
                                        throw new IllegalStateException("Use grouping collector factory instead");
                                    }
                                },
                                // combiner: combine two accumulators, but again we can't set the major here
                                MajorStatsAccumulator::combine,
                                // finisher: convert the accumulator to MajorStats, but we can't set the major here
                                MajorStatsAccumulator::toStats
                        )
                )
        );
    }

    /*
     * Since each group key is only known inside groupingBy, the cleanest way
     * to use Collector.of with a record result is to build the downstream collector
     * without storing the major in the accumulator, then inject the major afterward.
     *
     * The method above shows why tying the key into the accumulator directly is awkward.
     * So here is the correct implementation.
     */

    static Map<String, MajorStats> buildMajorStatsByMajorFixed(java.util.stream.Stream<Student> stream) {
        Map<String, MajorStatsWithoutKey> temp = stream.collect(
                Collectors.groupingBy(
                        Student::major,
                        Collector.of(
                                MajorStatsWithoutKeyAccumulator::new,
                                MajorStatsWithoutKeyAccumulator::add,
                                MajorStatsWithoutKeyAccumulator::combine,
                                MajorStatsWithoutKeyAccumulator::toStats
                        )
                )
        );

        return temp.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new MajorStats(
                                e.getKey(),
                                e.getValue().studentCount(),
                                e.getValue().avgGpa(),
                                e.getValue().uniqueCourses()
                        )
                ));
    }

    record MajorStatsWithoutKey(long studentCount, double avgGpa, Set<String> uniqueCourses) {}

    static class MajorStatsWithoutKeyAccumulator {
        private long studentCount = 0;
        private double totalGpa = 0.0;
        private final Set<String> uniqueCourses = new HashSet<>();

        void add(Student s) {
            studentCount++;
            totalGpa += s.gpa();
            uniqueCourses.addAll(s.courses());
        }

        MajorStatsWithoutKeyAccumulator combine(MajorStatsWithoutKeyAccumulator other) {
            this.studentCount += other.studentCount;
            this.totalGpa += other.totalGpa;
            this.uniqueCourses.addAll(other.uniqueCourses);
            return this;
        }

        MajorStatsWithoutKey toStats() {
            double avg = studentCount == 0 ? 0.0 : totalGpa / studentCount;
            return new MajorStatsWithoutKey(studentCount, avg, Set.copyOf(uniqueCourses));
        }
    }

    // Replace example 4 implementation with the correct one
    static Map<String, MajorStats> buildMajorStatsByMajorCorrect(java.util.stream.Stream<Student> stream) {
        return buildMajorStatsByMajorFixed(stream);
    }

    // =========================================================
    // Utility printing
    // =========================================================

    static void printMajorStatsMap(Map<String, MajorStats> statsMap) {
        statsMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> System.out.println(entry.getKey() + " -> " + entry.getValue()));
    }

    static List<Student> sampleStudents() {
        return List.of(
                new Student("Alice", "CS", 3.90, List.of("CS101", "MATH201", "STAT210")),
                new Student("Bob", "Math", 3.60, List.of("MATH201", "STAT210", "PHYS101")),
                new Student("Cara", "CS", 3.80, List.of("CS101", "CS202", "STAT210")),
                new Student("Dan", "Biology", 3.40, List.of("BIO110", "CHEM101", "STAT210")),
                new Student("Eva", "Math", 3.95, List.of("MATH201", "CS101", "STAT310")),
                new Student("Finn", "CS", 3.20, List.of("CS202", "CS301", "MATH201")),
                new Student("Gina", "Biology", 3.70, List.of("BIO110", "CHEM101", "BIO220")),
                new Student("Hank", "Physics", 3.85, List.of("PHYS101", "MATH201", "CS101"))
        );
    }

    public static void main(String[] args) {
        List<Student> students = sampleStudents();

        System.out.println("=== DATASET ===");
        students.forEach(System.out::println);

        System.out.println("\n=== example 1 — reduce basics ===");
        DemoExample1(students);

        System.out.println("\n=== example 2 — reduce to a different type ===");
        DemoExample2(students);

        System.out.println("\n=== example 3 — parallel correctness ===");
        DemoExample3();

        System.out.println("\n=== example 4 — Capstone ===");
        DemoExample4(students);
    }
}
