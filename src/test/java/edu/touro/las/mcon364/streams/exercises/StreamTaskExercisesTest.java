package edu.touro.las.mcon364.streams.exercises;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamTaskExercisesTest {

    private final StreamTaskExercises exercises = new StreamTaskExercises();

    private List<Task> sampleTasks() {
        return List.of(
                new Task(1, "Write syllabus", Priority.HIGH, Status.TODO),
                new Task(2, "Grade quizzes", Priority.HIGH, Status.DONE),
                new Task(3, "Email students", Priority.MEDIUM, Status.IN_PROGRESS),
                new Task(4, "Update repo", Priority.LOW, Status.DONE),
                new Task(5, "Prepare slides", Priority.HIGH, Status.IN_PROGRESS),
                new Task(6, "Book room", Priority.MEDIUM, Status.TODO)
        );
    }

    private List<WorkItem> sampleWorkItems() {
        return List.of(
                WorkItem.of(
                        "W-101", "Build auth", Priority.HIGH, Status.IN_PROGRESS,
                        List.of("backend", "security"),
                        List.of("Avi", "Nora"),
                        Map.of("design", 2, "build", 5)
                ),
                WorkItem.of(
                        "W-102", "Write docs", Priority.MEDIUM, Status.DONE,
                        List.of("docs", "backend"),
                        List.of("Nora", "Liam"),
                        Map.of("draft", 2, "review", 1)
                ),
                WorkItem.of(
                        "W-103", "UI polish", Priority.LOW, Status.DONE,
                        List.of("frontend", "ux"),
                        List.of("Mia", "Nora"),
                        Map.of("mock", 1, "refine", 2)
                ),
                WorkItem.of(
                        "W-104", "Test coverage", Priority.HIGH, Status.TODO,
                        List.of("qa", "backend"),
                        List.of("Avi"),
                        Map.of("plan", 1)
                )
        );
    }

    @Test
    void highPriorityDescriptions_returnsDescriptionsInEncounterOrder() {
        assertEquals(
                List.of("Write syllabus", "Grade quizzes", "Prepare slides"),
                exercises.highPriorityDescriptions(sampleTasks())
        );
    }

    @Test
    void countByStatus_countsTasksPerStatus() {
        assertEquals(
                Map.of(
                        Status.TODO, 2L,
                        Status.IN_PROGRESS, 2L,
                        Status.DONE, 2L
                ),
                exercises.countByStatus(sampleTasks())
        );
    }

    @Test
    void descriptionsByPriority_groupsDescriptionsByPriority() {
        assertEquals(
                Map.of(
                        Priority.HIGH, List.of("Write syllabus", "Grade quizzes", "Prepare slides"),
                        Priority.MEDIUM, List.of("Email students", "Book room"),
                        Priority.LOW, List.of("Update repo")
                ),
                exercises.descriptionsByPriority(sampleTasks())
        );
    }

    @Test
    void partitionByDone_splitsTasksIntoTrueAndFalseBuckets() {
        Map<Boolean, List<Task>> result = exercises.partitionByDone(sampleTasks());

        assertEquals(
                List.of(
                        new Task(2, "Grade quizzes", Priority.HIGH, Status.DONE),
                        new Task(4, "Update repo", Priority.LOW, Status.DONE)
                ),
                result.get(true)
        );

        assertEquals(
                List.of(
                        new Task(1, "Write syllabus", Priority.HIGH, Status.TODO),
                        new Task(3, "Email students", Priority.MEDIUM, Status.IN_PROGRESS),
                        new Task(5, "Prepare slides", Priority.HIGH, Status.IN_PROGRESS),
                        new Task(6, "Book room", Priority.MEDIUM, Status.TODO)
                ),
                result.get(false)
        );
    }

    @Test
    void countDonePartition_countsDoneAndNotDone() {
        assertEquals(
                Map.of(true, 2L, false, 4L),
                exercises.countDonePartition(sampleTasks())
        );
    }

    @Test
    void groupByStatusThenPriority_buildsNestedGrouping() {
        assertEquals(
                Map.of(
                        Status.TODO, Map.of(
                                Priority.HIGH, List.of(new Task(1, "Write syllabus", Priority.HIGH, Status.TODO)),
                                Priority.MEDIUM, List.of(new Task(6, "Book room", Priority.MEDIUM, Status.TODO))
                        ),
                        Status.IN_PROGRESS, Map.of(
                                Priority.MEDIUM, List.of(new Task(3, "Email students", Priority.MEDIUM, Status.IN_PROGRESS)),
                                Priority.HIGH, List.of(new Task(5, "Prepare slides", Priority.HIGH, Status.IN_PROGRESS))
                        ),
                        Status.DONE, Map.of(
                                Priority.HIGH, List.of(new Task(2, "Grade quizzes", Priority.HIGH, Status.DONE)),
                                Priority.LOW, List.of(new Task(4, "Update repo", Priority.LOW, Status.DONE))
                        )
                ),
                exercises.groupByStatusThenPriority(sampleTasks())
        );
    }

    @Test
    void sortedDescriptionsByStatus_groupsAndSortsDescriptionsAlphabetically() {
        assertEquals(
                Map.of(
                        Status.TODO, List.of("Book room", "Write syllabus"),
                        Status.IN_PROGRESS, List.of("Email students", "Prepare slides"),
                        Status.DONE, List.of("Grade quizzes", "Update repo")
                ),
                exercises.sortedDescriptionsByStatus(sampleTasks())
        );
    }

    @Test
    void doneTaskSummary_joinsDoneDescriptionsInEncounterOrder() {
        assertEquals(
                "Grade quizzes, Update repo",
                exercises.doneTaskSummary(sampleTasks())
        );
    }

    @Test
    void allTags_flattensTagsInEncounterOrder() {
        assertEquals(
                List.of("backend", "security", "docs", "backend", "frontend", "ux", "qa", "backend"),
                exercises.allTags(sampleWorkItems())
        );
    }

    @Test
    void distinctDoneAssignees_returnsDistinctAssigneesForDoneItemsInOrder() {
        assertEquals(
                List.of("Nora", "Liam", "Mia"),
                exercises.distinctDoneAssignees(sampleWorkItems())
        );
    }

    @Test
    void idToStatus_buildsMapFromIdToStatus() {
        assertEquals(
                Map.of(
                        "W-101", Status.IN_PROGRESS,
                        "W-102", Status.DONE,
                        "W-103", Status.DONE,
                        "W-104", Status.TODO
                ),
                exercises.idToStatus(sampleWorkItems())
        );
    }

    @Test
    void titlesByPriorityUsingMapping_groupsTitlesByPriority() {
        assertEquals(
                Map.of(
                        Priority.HIGH, List.of("Build auth", "Test coverage"),
                        Priority.MEDIUM, List.of("Write docs"),
                        Priority.LOW, List.of("UI polish")
                ),
                exercises.titlesByPriorityUsingMapping(sampleWorkItems())
        );
    }
}
