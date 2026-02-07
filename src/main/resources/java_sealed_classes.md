How Switch Expressions Improve Upon Classic Switch

Java 14+ introduces switch expressions, which are more powerful, safer, and more concise than traditional switch statements.

🔹 1. They return values

Classic switch = only a statement, cannot return a result.

int result = switch (day) {
    case MONDAY -> 1;
    case TUESDAY -> 2;
    default -> 0;
};


This was impossible in classic switch.

🔹 2. No fall-through by default

Classic switch falls through unless you add break:

switch(x) {
    case 1:
        // falls through accidentally
    case 2:
        ...
}


Switch expressions do not fall through, eliminating an entire class of bugs.

switch(x) {
    case 1 -> "one";
    case 2 -> "two";
}

🔹 3. Better, clearer syntax — “arrow” (->) style

New switch:

case MONDAY -> "Start";


Old switch:

case MONDAY:
    return "Start";


Cleaner + more readable.

🔹 4. Pattern Matching Support (modern Java)

Switch expressions integrate with pattern matching, enabling type-based dispatch.

Example:

switch (obj) {
    case String s -> s.toUpperCase();
    case Integer i -> i * 2;
    default -> 0;
}


Classic switch cannot operate on anything except:

primitives

enum

String

Switch expressions can handle types, not just values.

🔹 5. Exhaustiveness Checking

Compiler ensures all possible cases are handled if switching on a sealed type or an enum.

Example with sealed class:

int area = switch(shape) {
    case Circle c -> compute(c);
    case Rectangle r -> compute(r);
}; // no default needed


Classic switch does not enforce this.

🔹 6. yield keyword for complex cases

For multi-line logic:

int num = switch(x) {
    case 1 -> 10;
    case 2 -> {
        int val = compute();
        yield val;  // returns a value from block
    }
    default -> 0;
};