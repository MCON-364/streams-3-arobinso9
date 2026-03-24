package edu.touro.las.mcon364.streams.exercises;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Stream practice focused on collecting, grouping, and partitioning.
 *
 * Implement each method using streams.
 * Don't use loops.
 */
public class StreamTaskExercises {

    /**
     * Basics refresher:
     * Return the descriptions of all HIGH priority tasks in encounter order.
     */
    public List<String> highPriorityDescriptions(List<Task> tasks) {
        return tasks.stream()
                .filter(task -> task.priority() == Priority.HIGH)
                .map(Task::description)
                .toList();
    }

    /**
     * Collecting + grouping:
     * Return the number of tasks in each status.
     */
    public Map<Status, Long> countByStatus(List<Task> tasks) {
        return tasks.stream().collect(Collectors.groupingBy(Task::status, Collectors.counting()));
    }

    /**
     * Grouping + downstream mapping:
     * Group tasks by priority, but keep only task descriptions.
     */
    public Map<Priority, List<String>> descriptionsByPriority(List<Task> tasks) {
        return tasks.stream()
                .collect(Collectors.groupingBy(
                        Task::priority,
                        Collectors.mapping(Task::description, Collectors.toList())
                ));
    }

    /**
     * Partitioning:
     * Partition tasks into DONE and not DONE.
     * The map keys should be true and false.
     */
    public Map<Boolean, List<Task>> partitionByDone(List<Task> tasks) {
        return tasks.stream()
                .collect(Collectors.partitioningBy(task -> task.status() == Status.DONE));
    }

    /**
     * Partitioning + downstream counting:
     * Count how many tasks are DONE vs not DONE.
     */
    public Map<Boolean, Long> countDonePartition(List<Task> tasks) {
        return tasks.stream()
                .collect(Collectors.partitioningBy(
                        task -> task.status() == Status.DONE,
                        Collectors.counting()
                ));
    }

    /**
     * Nested grouping:
     * First group by status, then by priority.
     */
    public Map<Status, Map<Priority, List<Task>>> groupByStatusThenPriority(List<Task> tasks) {
        return tasks.stream()
                .collect(Collectors.groupingBy(
                        Task::status,
                        Collectors.groupingBy(Task::priority)
                ));
    }

    /**
     * Grouping + mapping + collectingAndThen:
     * Group by status and return alphabetically sorted descriptions for each status.
     */
    public Map<Status, List<String>> sortedDescriptionsByStatus(List<Task> tasks) {
        return tasks.stream()
                .collect(Collectors.groupingBy(
                        Task::status,
                        Collectors.collectingAndThen(
                                Collectors.mapping(Task::description, Collectors.toList()),
                                list -> list.stream().sorted().toList()
                        )
                ));
    }

    /**
     * Challenge:
     * Return a comma-separated string of descriptions for DONE tasks,
     * preserving encounter order.
     *
     * Example: "Write syllabus, Grade quizzes"
     */
    public String doneTaskSummary(List<Task> tasks) {
        return tasks.stream()
                .filter(task -> task.status() == Status.DONE)
                .map(Task::description)
                .collect(Collectors.joining(", "));
    }

    /**
     * flatMap:
     * Return all tags from all work items in encounter order.
     */
    public List<String> allTags(List<WorkItem> items) {
        return items.stream()
                .flatMap(item -> item.tags().stream())
                .toList();
    }

    /**
     * flatMap + distinct:
     * Return distinct assignees for DONE items in encounter order.
     */
    public List<String> distinctDoneAssignees(List<WorkItem> items) {
        return items.stream()
                .filter(item -> item.status() == Status.DONE)
                .flatMap(item -> item.assignees().stream())
                .distinct()
                .toList();
    }

    /**
     * toMap:
     * Build a map from work-item id to status.
     */
    public Map<String, Status> idToStatus(List<WorkItem> items) {
        return items.stream()
                .collect(Collectors.toMap(
                        WorkItem::id,
                        WorkItem::status
                ));
    }

    /**
     * groupingBy + mapping:
     * Group by priority and collect only titles.
     */
    public Map<Priority, List<String>> titlesByPriorityUsingMapping(List<WorkItem> items) {
        return items.stream()
                .collect(Collectors.groupingBy(
                        WorkItem::priority,
                        Collectors.mapping(WorkItem::title, Collectors.toList())
                ));
    }
}
