package edu.touro.las.mcon364.streams.demo;

import java.util.*;
import java.util.stream.*;

public class Demo {

    // ──────────────────────────────────────────────
    //  Domain Model
    // ──────────────────────────────────────────────

    record Student(
            String name,
            int age,
            String major,
            double gpa,
            List<String> enrolledCourses
    ) {
        @Override
        public String toString() {
            return name;
        }
    }

    // ──────────────────────────────────────────────
    //  Sample Data
    // ──────────────────────────────────────────────

    private static List<Student> getStudents() {
        return List.of(
                new Student("Alice",   21, "CS",      3.8, List.of("CS101", "CS201", "MATH301")),
                new Student("Bob",     22, "CS",      3.2, List.of("CS101", "CS301")),
                new Student("Charlie", 20, "Math",    3.9, List.of("MATH301", "MATH201", "CS101")),
                new Student("Diana",   23, "Math",    3.5, List.of("MATH201", "PHYS101")),
                new Student("Eve",     21, "Physics", 3.7, List.of("PHYS101", "MATH301", "CS201")),
                new Student("Frank",   24, "CS",      2.9, List.of("CS101")),
                new Student("Grace",   20, "Physics", 3.6, List.of("PHYS101", "PHYS201", "MATH201"))
        );
    }

    // ══════════════════════════════════════════════
    //  EXAMPLE 1 — groupingBy with downstream
    // ══════════════════════════════════════════════

    /**
     * 1a. Group students by major and find the maximum GPA in each group.
     *
     *     Collectors.groupingBy  groups by major
     *     Collectors.maxBy(...) takes a comparator to find the student with the max GPA in each major
     *     We will use maxBy as the downstream collector for groupingBy
     *     Expected output:
     *     {CS=Optional[Alice], Math=Optional[Charlie], Physics=Optional[Eve]}
     */
    static Map<String, Optional<Student>> maxGpaByMajor(List<Student> students) {
        return students.stream().collect(Collectors.groupingBy(Student::major, Collectors.maxBy(Comparator.comparingDouble(Student::gpa))));

    }

    /**
     * 1b. Group students by major and collect a sorted set of their names.
     *
     *     Collectors.groupingBy by major to group student
     *     Collectors.mapping to extract student names from each group
     *           and Collectors.toCollection(TreeSet::new) to create a sorted set of names
     *     
     *     Expected output:
     *     {CS=[Alice, Bob, Frank], Math=[Charlie, Diana], Physics=[Eve, Grace]}
     */
    static Map<String, TreeSet<String>> sortedNamesByMajor(List<Student> students) {
         return students.stream().collect(Collectors.groupingBy(Student::major, Collectors.mapping(Student::name, Collectors.toCollection(TreeSet::new))));
    }

    // ══════════════════════════════════════════════
    //  EXAMPLE 2 — toMap
    // ══════════════════════════════════════════════

    /**
     * 2a. Create a map of studentName → numberOfCoursesEnrolled.
     *
     *     Expected output:
     *     {Alice=3, Bob=2, Charlie=3, Diana=2, Eve=3, Frank=1, Grace=3}
     */
    static Map<String, Integer> courseCountByName(List<Student> students) {
        return students.stream().collect(Collectors.toMap(Student::name,s -> s.enrolledCourses().size()));

    }

    /**
     * 2b. Create a map of major → totalEnrollments
     *     (sum of all courses across all students in that major).
     *
     *     Reminder: We need a merge function since multiple students share a major.
     *     so we will use sum method on Integer class
     *
     *     Expected output:
     *     {CS=6, Math=5, Physics=6}
     */
    static Map<String, Integer> totalEnrollmentsByMajor(List<Student> students) {
        return students.stream()
                .collect(Collectors.toMap(
                        Student::major,
                        s -> s.enrolledCourses().size(),
                        Integer::sum
                ));
    }

    // ══════════════════════════════════════════════
    //  EXAMPLE 3 — flatMap
    // ══════════════════════════════════════════════

    /**
     * 3a. Find the most popular course — the course code that appears
     *     in the most students' enrollment lists.
     *     Return a Map.Entry with the course code and its count.
     *
     *     Hint: flatMap to get all course codes, then group and count.
     *
     *     Expected output:
     *     CS101=4
     */
    static Optional<Map.Entry<String, Long>> mostPopularCourse(List<Student> students) {
        // TODO: implement using flatMap + groupingBy + counting, then find the max
        return Optional.empty();
    }

    /**
     * 3b. Find all pairs of students who share at least one course.
     *     Return a Set of descriptions like "Alice & Bob both take CS101".
     *
     *     Step 1:
     *     - Use flatMap to produce (student, course) pairs
     *     - Group by course to get a list of student names per course
     *     Step 2:
     *     - For each course with 2+ students, generate all pair descriptions
     *     - To avoid duplicates like "Bob & Alice" and "Alice & Bob",
     *       only create pairs where the first name comes before the second alphabetically
     *
     *     Expected output (partial):
     *     [Alice & Bob both take CS101, Alice & Charlie both take CS101, ...]
     */
    static Set<String> sharedCoursePairs(List<Student> students) {
        // Step 1: Build a map of course → list of student names
        Map<String, List<String>> courseToStudents = students.stream()
                .flatMap(s -> s.enrolledCourses().stream()
                        .map(course -> Map.entry(course, s.name()))
                )
                .collect(Collectors.groupingBy(
                        Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())
                ));

        // Step 2: For each course with 2+ students, generate alphabetically ordered pairs
        return courseToStudents.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .flatMap(e -> {
                    List<String> names = e.getValue();
                    String course = e.getKey();
                    return names.stream()
                            .flatMap(a -> names.stream()
                                    .filter(b -> a.compareTo(b) < 0)       // alphabetical order, no self-pairs
                                    .map(b -> a + " & " + b + " both take " + course)
                            );
                })
                .collect(Collectors.toSet());
    }
    // ══════════════════════════════════════════════
    //  EXAMPLE 4 — Challenge: multi-level grouping
    // ══════════════════════════════════════════════

    /**
     * 4. Group students first by major, then within each major by whether
     *    they are honors (GPA ≥ 3.5).
     *
     *    Hint: nested groupingBy and partitioningBy
     *
     *    Expected output:
     *    {
     *      CS      = {false=[Bob, Frank], true=[Alice]},
     *      Math    = {false=[], true=[Charlie, Diana]},
     *      Physics = {false=[], true=[Eve, Grace]}
     *    }
     */
    static Map<String, Map<Boolean, List<Student>>> honorsByMajor(List<Student> students) {
        return students.stream()
                .collect(Collectors.groupingBy(
                        Student::major,
                        Collectors.partitioningBy(s -> s.gpa() >= 3.5)
                ));

    }

    // ──────────────────────────────────────────────
    //  Main — run and verify your solutions
    // ──────────────────────────────────────────────

    public static void main(String[] args) {
        List<Student> students = getStudents();

        System.out.println("=== Exercise 1a: Max GPA by Major ===");
        System.out.println(maxGpaByMajor(students));
        System.out.println();

        System.out.println("=== Exercise 1b: Sorted Names by Major ===");
        System.out.println(sortedNamesByMajor(students));
        System.out.println();

        System.out.println("=== Exercise 2a: Course Count by Name ===");
        System.out.println(courseCountByName(students));
        System.out.println();

        System.out.println("=== Exercise 2b: Total Enrollments by Major ===");
        System.out.println(totalEnrollmentsByMajor(students));
        System.out.println();

        System.out.println("=== Exercise 3a: Most Popular Course ===");
        mostPopularCourse(students).ifPresentOrElse(
                entry -> System.out.println(entry.getKey() + " (" + entry.getValue() + " students)"),
                () -> System.out.println("No courses found")
        );
        System.out.println();

        System.out.println("=== Exercise 3b: Shared Course Pairs ===");
        sharedCoursePairs(students).stream().sorted().forEach(System.out::println);
        System.out.println();

        System.out.println("=== Exercise 4: Honors by Major (multi-level) ===");
        honorsByMajor(students).forEach((major, split) -> {
            System.out.println("  " + major + ":");
            System.out.println("    Honors:     " + split.getOrDefault(true, List.of()));
            System.out.println("    Non-honors: " + split.getOrDefault(false, List.of()));
        });
    }
}