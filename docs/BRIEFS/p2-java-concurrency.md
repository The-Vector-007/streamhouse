# P2 brief — Java 21 concurrency and backpressure

You are learning Java here, so slow down. This phase is the one that opens the
roles that were closed to you.

## 1. Virtual threads, and what actually changed

Before Java 21, a thread was an OS thread: ~1 MB of stack, and a context switch
costs the kernel. So blocking on IO was expensive, and the whole async/reactive
industry existed to avoid it.

A virtual thread is scheduled by the JVM onto a small pool of carrier threads. When
it blocks on IO, the JVM parks it and runs another. Cost is a few hundred bytes.
You can have a million.

**The consequence that matters:** "thread per request" with blocking IO is a good
design again. You do not need reactive code to be fast.

```java
// This is now fine. It was not, in 2019.
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    executor.submit(() -> handleRequest(req));
}
```

Read:
- JEP 444: Virtual Threads
- *Java 21 virtual threads: pinning and what still blocks a carrier thread*

**The trap to know about:** a virtual thread inside a `synchronized` block can
*pin* its carrier thread, and you lose the benefit. Use `ReentrantLock` instead.
This is a favourite interview question because it separates people who read the
headline from people who read the JEP.

## 2. Backpressure, which is the real lesson

Virtual threads make it cheap to *accept* work. They do nothing to make it cheap to
*do* the work.

If 10,000 requests arrive and Kafka accepts 500/s, unbounded acceptance means
9,500 requests queue in memory, then 19,000, then the JVM dies. **You converted a
latency problem into an availability problem**, which is strictly worse: slow is
annoying, OOM is an outage.

The fix is a **bounded** queue and an explicit rejection policy:

```java
// The bound IS the design. Picking 10_000 is a claim about how much
// latency you will tolerate before shedding load; be ready to defend it.
var queue = new ArrayBlockingQueue<Task>(10_000);
if (!queue.offer(task)) {
    return response(429, "retry-after: 1");
}
```

Returning 429 looks like failure and is actually correct: you told the client the
truth quickly instead of lying slowly and dying.

Read:
- *Little's Law* — L = λW. Ten minutes on this pays off for the rest of your career.
- Google SRE Book, *Handling Overload*

## 3. Producer durability

`acks` decides what "the broker got it" means:

| `acks` | Returns when | You lose data if |
|---|---|---|
| `0` | the socket accepted the bytes | anything at all goes wrong |
| `1` | the leader wrote it | the leader dies before replicas catch up |
| `all` | all in-sync replicas wrote it | you lose every replica |

With `enable.idempotence=true` the producer adds a sequence number per partition so
broker-side retries do not duplicate. That is *producer* idempotence — it does not
help when your *client* retries. That is P2.4's problem, and conflating the two is a
common interview stumble.

## 4. Graceful shutdown

SIGTERM arrives. If you exit immediately you lose in-flight requests and anything
buffered in the producer (the producer batches — that is why it is fast).

```
1. stop accepting new connections
2. let in-flight requests finish, with a deadline
3. producer.flush()          <- the one people forget
4. producer.close()
5. exit
```

Kubernetes sends SIGTERM then SIGKILL after a grace period, so your deadline must be
shorter than that grace period. Knowing this number exists puts you ahead of most
mid-level candidates.

## Answer these before you write code

1. Your gateway returns 200. Where exactly is the message at that moment, and what
   does `acks=all` change about the answer?
2. 10,000 concurrent requests arrive, Kafka slows to 500/s. What does your service
   do? What *should* it do?
3. A client retries a payment POST after a timeout. How do you avoid double-charging,
   and where does that state live?
4. SIGTERM with 200 requests in flight. What must happen before exit?
