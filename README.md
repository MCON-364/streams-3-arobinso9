# MCON 364 Test One Repo

## Overview
This test is designed to evaluate your Java coding skills that you built over last several lessons.

The problems are intentionally similar in **style and skill level** to the practice work you already completed on:
- functional interfaces
- very basic streams
- `Map`, `Optional`, and simple state changes with undo

The wording, class names, and scenarios are different, but the techniques are the same.

## Rules
- Use **Java 21**.
- Keep method signatures exactly as given.
- Do **not** rename packages, classes, or methods.
- Do **not** add external libraries.
- Prefer clear, direct solutions over clever ones.
- You only need to use streams is the problem states so. 
- Follow the comments inside each class.

## Test Structure
There are **15 graded problems** total.

### Part A — FunctionalWarmup (5 problems)
1. Build a `Supplier<Integer>` for the current month number.
2. Build a `Predicate<String>` that checks whether a string is longer than 5 characters.
3. Build a chained `Predicate<Integer>` that checks whether a number is positive and even.
4. Build a `Function<String, Integer>` that counts words in a string.
5. Process a list of labels by filtering, trimming, uppercasing, and collecting.

### Part B — BasicStreamsQuiz (5 problems)
6. Return a sorted list of all course names.
7. Count how many scores are at least a threshold.
8. Return the first word longer than a given length as an `Optional`.
9. Square every number in a list using streams.
10. Compute the average of passing scores.

### Part C — StudyTracker (5 problems)
11. Add a learner.
12. Add a score for an existing learner (undoable).
13. Compute an average for one learner using `Optional`.
14. Convert that average into a letter band.
15. Undo the last state-changing operation.

## Tips
- `Supplier`, `Predicate`, `Function`, and `Consumer` should use lambdas or function references.
- For stream problems, use stream operations instead of manual loops.
- For the tracker section, read the method comments carefully before coding.

## Run tests locally to make sure your code compiles
```bash
mvn test
```

## Files to complete
- `FunctionalWarmup.java`
- `BasicStreamsQuiz.java`
- `StudyTracker.java`

Good luck.
