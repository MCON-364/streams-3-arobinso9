package edu.touro.las.mcon364.streams.exercises;

/**
 * Immutable task model (Java 16+ records).
 * We use it to make undo/redo easy: "before" snapshots are just values.
 */
public record Task(int id, String description, Priority priority, Status status) {

    public Task {
        if (id <= 0) throw new IllegalArgumentException("id must be > 0");
        if (description == null || description.isBlank()) {
            throw new IllegalArgumentException("description must be non-blank");
        }
        if (priority == null) throw new IllegalArgumentException("priority must not be null");
        if (status == null) throw new IllegalArgumentException("status must not be null");
    }

    public Task withDescription(String newDescription) {
        return new Task(id, newDescription, priority, status);
    }

    public Task withPriority(Priority newPriority) {
        return new Task(id, description, newPriority, status);
    }

    public Task withStatus(Status newStatus) {
        return new Task(id, description, priority, newStatus);
    }
}
