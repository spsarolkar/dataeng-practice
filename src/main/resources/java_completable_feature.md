CompletableFuture in Java (Complete Guide)

CompletableFuture was introduced in Java 8 as part of java.util.concurrent.
It provides a powerful, non-blocking, asynchronous programming model.

🔹 1. What is CompletableFuture?

A CompletableFuture represents a value that may not yet be available.
It allows you to write asynchronous, callback-driven code without blocking threads.

Example:

CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
    return "Hello!";
});

🔹 2. Core Features
✅ Asynchronous execution
CompletableFuture.runAsync(() -> System.out.println("Running async"));

✅ Returning a value
CompletableFuture<Integer> future =
        CompletableFuture.supplyAsync(() -> 10);

🔹 3. Transforming Results
👉 thenApply()

Transforms the result synchronously (same thread when possible):

future.thenApply(n -> n * 2);

👉 thenApplyAsync()

Runs transformation in the ForkJoinPool (different thread):

future.thenApplyAsync(n -> n * 2);

🔹 4. Chaining Asynchronous Computations
👉 thenCompose()

Used when the next step depends on previous result (flatMap equivalent):

CompletableFuture<String> result =
        getUser()
        .thenCompose(user -> getOrders(user.id));

🔹 5. Combining Independent Futures
👉 thenCombine()

Runs two futures in parallel and combines results:

future1.thenCombine(future2, (a, b) -> a + b);

🔹 6. Running Futures in Parallel
CompletableFuture<Void> all =
        CompletableFuture.allOf(f1, f2, f3);

🔹 7. Handling Errors
👉 exceptionally()
future.exceptionally(ex -> {
    System.out.println("Error: " + ex);
    return -1;
});

👉 handle()

Runs whether exception occurred or not:

future.handle((result, ex) -> {
    if (ex != null) return -1;
    return result * 2;
});

🔹 8. Completing Futures Manually
CompletableFuture<String> f = new CompletableFuture<>();

f.complete("done");  // manually completes


Useful for integrating async callbacks.

🔹 9. Delayed/Scheduled Execution (Java 9+)
👉 delayedExecutor()

Useful for retries, timeouts, throttling:

CompletableFuture.delayedExecutor(3, TimeUnit.SECONDS)
    .execute(() -> System.out.println("runs after 3 sec"));

🔹 10. Timeouts (Java 9+)
future.orTimeout(2, TimeUnit.SECONDS);
future.completeOnTimeout("default", 2, TimeUnit.SECONDS);

🔹 11. Common Threading Pitfalls
🚫 Blocking get() inside async code

Avoid:

future.thenApply(x -> future.get()); // deadlock risk

🚫 Too many tasks in ForkJoinPool

Use custom executor:

ExecutorService ex = Executors.newFixedThreadPool(10);
CompletableFuture.supplyAsync(() -> ..., ex);

🔹 12. Real-world Example: Fetch User + Orders in Parallel
CompletableFuture<User> userFuture = CompletableFuture.supplyAsync(() -> getUser());
CompletableFuture<List<Order>> ordersFuture = CompletableFuture.supplyAsync(() -> getOrders());

CompletableFuture<UserProfile> result =
    userFuture.thenCombine(ordersFuture,
        (user, orders) -> new UserProfile(user, orders)
    );

🔥 Interview Questions You Should Expect

✔ What is the difference between thenApply, thenCompose, and thenCombine?
✔ How does CompletableFuture differ from Future?
✔ What is the use of allOf() and anyOf()?
✔ What is the role of ForkJoinPool.commonPool()?
✔ How do you handle timeouts?
✔ What happens if a stage throws an exception?
✔ What is the difference between Async vs Non-Async methods?