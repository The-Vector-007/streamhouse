# P2 primer — Java for someone who writes Python

Read this before `p2-java-concurrency.md`. It covers the things that will cost you
an afternoon each if nobody warns you.

Good news first: you have C++ on your resume. C++ → Java is a much shorter jump
than Python → Java. Static types, classes, compilation and manual reasoning about
memory layout are already in your head. What is genuinely new is the ecosystem and
one very large concurrency idea (§9).

---

## 1. The compile step changes your feedback loop

Python fails at the line that runs. Java fails at the line that *exists*. A typo in
a branch you never take is a build error, not a 3am surprise.

```
mvn compile     # javac -> target/classes
mvn test        # compile + run tests
mvn package     # + build the jar
```

Maven has a fixed **lifecycle**: `validate → compile → test → package → verify →
install → deploy`. Running a later phase runs every earlier one. `mvn test` always
compiles first, so there is no stale-bytecode trap.

Layout is not a convention you may ignore — Maven *requires* it:

```
src/main/java/com/streamhouse/ingest/Gateway.java
src/test/java/com/streamhouse/ingest/GatewayTest.java
```

**Package must match directory.** `package com.streamhouse.ingest;` has to live in
`com/streamhouse/ingest/`. Python lets a module sit anywhere on `sys.path`; Java
does not.

## 2. No truthiness

```python
if items:          # empty list is falsy
```
```java
if (!items.isEmpty())   // "if (items)" does not compile
```

`if` takes a `boolean`. Not an int, not a reference. This removes a whole class of
bug and will annoy you for about a day.

## 3. `==` is not `.equals()`

The single most common Python-to-Java bug.

```java
String a = "acc_1", b = "acc_" + accountId;
a == b          // reference identity. usually false. sometimes true, worse.
a.equals(b)     // value equality. what you meant.
```

`==` compares references for objects, values for primitives. Python's `==` calls
`__eq__`; Java's does not. Use `Objects.equals(a, b)` when either side may be null.

## 4. Primitives and their box types

`int` is a value; `Integer` is an object that can be null. Autoboxing converts
silently, which is convenient right up until it is not:

```java
Map<String, Integer> counts = new HashMap<>();
int n = counts.get("missing");   // NullPointerException, not 0
```

Collections cannot hold primitives, so a `List<Integer>` boxes every element. In a
hot loop that is real allocation — worth knowing when you get to P7.

## 5. Checked exceptions, which have no Python analogue

Java splits exceptions in two:

- **Checked** (`IOException`): the compiler forces you to `catch` or declare `throws`.
- **Unchecked** (`RuntimeException`, `NullPointerException`): like Python's.

```java
void send(Record r) throws IOException { ... }   // callers MUST deal with it
```

The design intent: recoverable failures are part of a method's contract. The
practical reality: people wrap everything in `RuntimeException` to shut the
compiler up, which throws away the intent. Have a view on this — it is a genuine
interview conversation, not a trivia question.

## 6. `null` is not `None`

`null` has no type and no methods. Calling anything on it is an NPE. There is no
`Optional` by default, though `Optional<T>` exists and is idiomatic as a *return*
type (not as a field or parameter).

Coming from Python, your instinct is `if x is None`. In Java prefer designing so
null cannot occur: `Objects.requireNonNull` at construction, immutable fields,
empty collections instead of null ones.

## 7. Records replace your dataclasses

```python
@dataclass(frozen=True)
class Transaction:
    id: str
    amount_minor: int
```
```java
public record Transaction(String id, long amountMinor) {}
```

You get the constructor, accessors (`t.id()`, no `get` prefix), `equals`,
`hashCode` and `toString`. Add validation in a compact constructor:

```java
public record Transaction(String id, long amountMinor) {
    public Transaction {
        if (amountMinor < 0) throw new IllegalArgumentException("negative amount");
    }
}
```

## 8. Interfaces, `var`, and try-with-resources

```java
List<String> ids = new ArrayList<>();   // declare the interface, construct the impl
var ids = new ArrayList<String>();      // var infers; locals only, never fields
```

`try`-with-resources is Python's `with`:

```java
try (var producer = new KafkaProducer<String, String>(props)) {
    producer.send(record);
}   // close() called, even on exception
```

## 9. The big one: there is no GIL

In Python, threads do not run bytecode in parallel. You reach for `multiprocessing`
for CPU work, and shared mutable state between threads is *mostly* safe by accident.

**In Java, threads genuinely run at the same time on different cores.** Two threads
incrementing the same `int` will lose updates. Not rarely. Constantly.

```java
private int count;                // broken under concurrency
private final AtomicInteger count = new AtomicInteger();   // fine
```

Everything you are about to learn in P2 — the bounded queue, `AtomicInteger`,
`ConcurrentHashMap`, why `synchronized` pins a virtual thread — exists because this
is true. You have never had to think about it in Python, and it is the single
biggest mental shift in this phase.

This is also why interviewers ask Java candidates about concurrency and rarely ask
Python candidates. Take the time here.

## 10. What to skip for now

- **Spring Boot.** Deliberately not used in this project — you want to see the
  machinery. Know that a large share of Java job postings really mean Spring, and
  that is a later, separate investment.
- **Lombok.** Records cover most of what you would want it for.
- **Streams API.** Nice, and comprehension-like, but do not let it become the point.
  Write the loop first.

## Reading

- *Effective Java* (Bloch), items 10 (`equals`), 17 (immutability), 78–83 (concurrency).
  Do not read it cover to cover; treat it as a reference.
- Java 21 docs: `java.util.concurrent` package summary.
- JEP 444 (virtual threads) — after §9 above makes sense.

## Before you start P2.1

Write in `docs/ANSWERS.md`:

> *"In Python I could increment a counter from two threads and usually get away with
> it. Why is that not true in Java, and what would I use instead?"*

If you can answer that cold, §9 has landed and the rest of P2 is mechanics.
