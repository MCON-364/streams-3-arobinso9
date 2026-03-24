package edu.touro.las.mcon364.streams.exercises;

import java.util.List;
import java.util.Map;

/**
 * Richer domain object for realistic stream exercises.
 *
 * Includes multi-valued fields so flatMap/mapping/toMap use-cases are natural.
 */
public record WorkItem(
        String id,
        String title,
        Priority priority,
        Status status,
        List<String> tags,
        List<String> assignees,
        Map<String, Integer> estimatesByPhase
) {

    public WorkItem {
        tags = List.copyOf(tags);
        assignees = List.copyOf(assignees);
        estimatesByPhase = Map.copyOf(estimatesByPhase);
    }

    public static WorkItem of(
            String id,
            String title,
            Priority priority,
            Status status,
            List<String> tags,
            List<String> assignees,
            Map<String, Integer> estimatesByPhase
    ) {
        return new WorkItem(id, title, priority, status, tags, assignees, estimatesByPhase);
    }
}

