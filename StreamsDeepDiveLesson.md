# Deep Dive Lesson: Java Streams Beyond the Basics

## Lesson Title
Deepening Your Understanding of Java Streams: Collecting, Grouping, and Partitioning

 
## Learning Objectives
By the end of this lesson, students should be able to:

1. Review how streams work as pipelines over collections.
2. Distinguish between intermediate and terminal operations.
3. Use `collect(...)` to transform stream results into useful data structures.
4. Use `Collectors.groupingBy(...)` to organize data by categories.
5. Use `Collectors.partitioningBy(...)` to split data into true/false buckets.
6. Combine collectors such as `mapping`, `counting`, and `collectingAndThen`.
7. Read a stream pipeline and explain what shape of data it produces.

---

## Part 1 - Stream Basics Refresher (10-15 minutes)

A Java stream is a pipeline for processing data from a source such as a `List`, `Set`, or array. Streams do not store data themselves; they operate on data that already exists in a collection or another source. A stream pipeline usually has three parts: a source, one or more intermediate operations, and a terminal operation. Intermediate operations such as `filter`, `map`, and `sorted` produce a new stream and are lazy, which means they do not do work until a terminal operation runs. Terminal operations such as `toList`, `collect`, `count`, and `forEach` finish the pipeline and produce a result or side effect. Streams are especially useful when we want to describe *what* transformation we want instead of manually controlling every loop.

### Example: basic filtering and mapping

```java
List<String> descriptions = tasks.stream()
        .filter(task -> task.priority() == Priority.HIGH)
        .map(Task::description)
        .toList();
```

### Review talking points

- `stream()` creates the pipeline.
- `filter(...)` keeps only matching elements.
- `map(...)` transforms each element.
- `toList()` is the terminal operation that materializes the result.
- The original list is unchanged.

### Quick check for students

Ask students:

- What is the source here?
- Which operations are intermediate?
- Which operation is terminal?
- What is the final type of the result?

Answer: `List<String>`.

---

## Part 2 - Why `collect(...)` Matters (10 minutes)

Students often stop at `toList()`, but the real power of streams appears when they use `collect(...)`. The `collect(...)` terminal operation allows us to gather the results of a stream into more complex structures such as maps, grouped lists, counted summaries, or nested results.

### Simple collecting example

```java
Set<Status> statuses = tasks.stream()
        .map(Task::status)
        .collect(Collectors.toSet());
```

### Counting with collectors

```java
long highCount = tasks.stream()
        .filter(task -> task.priority() == Priority.HIGH)
        .collect(Collectors.counting());
```

This is similar to `.count()`, but it becomes very useful when nested inside another collector.

### Key teaching idea

`collect(...)` is often the bridge between a stream pipeline and a real application result.

---

## Part 3 - Grouping with `groupingBy(...)` (20-25 minutes)

Grouping is one of the most important stream skills because it lets students turn a flat list into a structured view of the data.

### Basic grouping

```java
Map<Status, List<Task>> byStatus = tasks.stream()
        .collect(Collectors.groupingBy(Task::status));
```

This answers the question: **Which tasks belong to each status?**

### Grouping with counting

```java
Map<Status, Long> countByStatus = tasks.stream()
        .collect(Collectors.groupingBy(
                Task::status,
                Collectors.counting()
        ));
```

This changes the value type from `List<Task>` to `Long`.

### Grouping with mapping

```java
Map<Priority, List<String>> descriptionsByPriority = tasks.stream()
        .collect(Collectors.groupingBy(
                Task::priority,
                Collectors.mapping(Task::description, Collectors.toList())
        ));
```

This answers: **For each priority, what are the descriptions?**

### Nested grouping

```java
Map<Status, Map<Priority, List<Task>>> nested = tasks.stream()
        .collect(Collectors.groupingBy(
                Task::status,
                Collectors.groupingBy(Task::priority)
        ));
```

This is powerful, but students must carefully track the result type.

### Teaching pause

Have students say the result type out loud before they run the code.

---

## Part 4 - Partitioning with `partitioningBy(...)` (15 minutes)

Partitioning is like grouping, but specifically for a boolean condition. The result is always a map with two keys: `true` and `false`.

### Example: done vs not done

```java
Map<Boolean, List<Task>> partitioned = tasks.stream()
        .collect(Collectors.partitioningBy(task -> task.status() == Status.DONE));
```

### Partitioning with counting

```java
Map<Boolean, Long> doneCounts = tasks.stream()
        .collect(Collectors.partitioningBy(
                task -> task.status() == Status.DONE,
                Collectors.counting()
        ));
```

### Important comparison

- Use `groupingBy(...)` when the classifier has many categories.
- Use `partitioningBy(...)` when the classifier is a yes/no question.

### Common confusion

Students often use `groupingBy(task -> task.status() == Status.DONE)` when `partitioningBy(...)` would be clearer.

---

## Part 5 - Collector Composition (15 minutes)

The deep understanding comes from combining collectors.

### Example: group tasks by status, but keep only descriptions

```java
Map<Status, List<String>> result = tasks.stream()
        .collect(Collectors.groupingBy(
                Task::status,
                Collectors.mapping(Task::description, Collectors.toList())
        ));
```

### Example: group by status and count

```java
Map<Status, Long> result = tasks.stream()
        .collect(Collectors.groupingBy(
                Task::status,
                Collectors.counting()
        ));
```

### Example: group by priority and get immutable sorted descriptions

```java
Map<Priority, List<String>> result = tasks.stream()
        .collect(Collectors.groupingBy(
                Task::priority,
                Collectors.collectingAndThen(
                        Collectors.mapping(Task::description, Collectors.toList()),
                        list -> list.stream().sorted().toList()
                )
        ));
```

### Why this matters

Students begin to see collectors as building blocks:

- **groupingBy** decides the buckets
- **mapping** decides what each element becomes
- **counting** decides how the bucket is summarized
- **collectingAndThen** applies one final transformation

---

## Part 6 - Common Mistakes (5-10 minutes)

### Mistake 1: forgetting the result type changes

```java
Collectors.groupingBy(Task::status)
```

returns:

```java
Map<Status, List<Task>>
```

but

```java
Collectors.groupingBy(Task::status, Collectors.counting())
```

returns:

```java
Map<Status, Long>
```

### Mistake 2: using `map(...)` when `filter(...)` is needed

`map(...)` transforms elements. `filter(...)` keeps or removes elements.

### Mistake 3: using `forEach(...)` when you really need a collected result

If the goal is to build a structure, `collect(...)` is usually better than mutating an external collection.

### Mistake 4: not reading nested generic types carefully

Students should narrate the type as they write it.

---

## Part 7 - In-Class Guided Practice

Use the `Task` record from your repo:

```java
Task t1 = new Task(1, "Write syllabus", Priority.HIGH, Status.TODO);
Task t2 = new Task(2, "Grade quizzes", Priority.HIGH, Status.DONE);
Task t3 = new Task(3, "Email students", Priority.MEDIUM, Status.IN_PROGRESS);
Task t4 = new Task(4, "Update repo", Priority.LOW, Status.DONE);
```

Ask students to write pipelines for:

1. All descriptions of DONE tasks.
2. Count of tasks per status.
3. Descriptions grouped by priority.
4. Tasks partitioned into DONE and not DONE.
5. Nested grouping by status, then priority.

---

## Part 8 - Exit Ticket Prompts

1. What is the difference between `groupingBy(...)` and `partitioningBy(...)`?
2. Why is `counting()` often used inside `groupingBy(...)`?
3. What does `mapping(...)` do inside another collector?
4. How can you tell the final type of a stream pipeline?

---

## Suggested Lesson Flow

- 10 min: refresher on stream basics
- 10 min: collecting overview
- 20 min: grouping with examples
- 15 min: partitioning with examples
- 15 min: guided practice
- 10 min: Kahoot and discussion

---

## Instructor Notes

Emphasize that strong stream users do not just memorize syntax. They learn to read a pipeline by repeatedly asking:

1. What is flowing through the stream right now?
2. What does this operation do to each element?
3. What is the final result type?

That habit is what turns streams from "fancy loops" into a real problem-solving tool.
