/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.java.impl.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background worker that fulfills {@link BatchRequest}s by fetching messages from a delegate
 * {@link SimpleConsumer} and buffering them locally.
 *
 * <p>This worker is generic: the same fetch loop serves both <strong>batch receive</strong>
 * (accumulate until count/bytes/timeout) and <strong>cached receive</strong> (return immediately
 * from overflow or after a single fetch) \u2014 the difference is entirely determined by the
 * {@link BatchRequest} parameters ({@code maxWaitNanos = 0} means no accumulation wait).
 *
 * <p><strong>Demand-driven</strong>: the worker blocks on {@link BlockingQueue#take()} when no
 * requests are pending, producing zero server traffic.
 *
 * <p><strong>Cache eviction</strong>: messages that sit idle in the overflow buffer longer than
 * {@code cacheEvictionTime} are proactively released back to the server so that other consumers
 * can process them.
 */
final class BatchFetchWorker {

    private static final Logger log = LoggerFactory.getLogger(BatchFetchWorker.class);

    /**
     * Number of messages the background worker attempts to pull per {@code receive()} call.
     */
    private static final int FETCH_BATCH_SIZE = 32;

    /**
     * Duration used when releasing evicted or shutdown-buffered messages.
     */
    private static final Duration RELEASE_INVISIBLE_DURATION = Duration.ofSeconds(1);

    private final SimpleConsumer delegate;
    private final Duration cacheEvictionTime;

    /**
     * Single-threaded executor that runs {@link #fetchLoop()}.
     */
    private final ExecutorService fetchExecutor;

    /**
     * Scheduler used exclusively for batch-timeout callbacks.
     */
    private final ScheduledExecutorService scheduler;

    /**
     * FIFO queue of pending batch requests.  The worker takes from the head; callers add to the
     * tail.  The worker blocks on {@link BlockingQueue#take()} when the queue is empty, which
     * implements the "demand-driven" semantics (no receive when no request).
     */
    private final BlockingQueue<BatchRequest> requestQueue = new LinkedBlockingQueue<>();

    /**
     * Overflow buffer: when a single {@code delegate.receive()} returns more messages than the
     * current batch request needs, the surplus is parked here for the next request.
     * Each entry records the timestamp when it was cached, enabling time-based eviction.
     */
    private final ConcurrentLinkedDeque<CachedMessage> overflowBuffer = new ConcurrentLinkedDeque<>();

    private volatile boolean closed = false;

    BatchFetchWorker(SimpleConsumer delegate, Duration cacheEvictionTime) {
        this.delegate = delegate;
        this.cacheEvictionTime = cacheEvictionTime;
        this.fetchExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "BatchingSimpleConsumer-fetch");
            t.setDaemon(true);
            return t;
        });
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "BatchingSimpleConsumer-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts the background fetch loop.  Must be called once after construction to satisfy
     * SpotBugs {@code SC_START_IN_CTOR}.
     */
    void start() {
        fetchExecutor.execute(this::fetchLoop);
        // Schedule periodic cache eviction.
        long evictionCheckMillis = Math.max(cacheEvictionTime.toMillis() / 3, 1000);
        scheduler.scheduleAtFixedRate(this::evictExpiredCache,
            evictionCheckMillis, evictionCheckMillis, TimeUnit.MILLISECONDS);
    }

    /** Enqueues a batch request.  Thread-safe. */
    void submit(BatchRequest request) {
        requestQueue.add(request);
    }

    /**
     * Shuts down the worker and returns any messages still buffered internally.
     *
     * <p>The caller is responsible for releasing the returned messages (e.g. shortening their
     * invisible duration).
     *
     * @return messages that were buffered in the overflow or in pending requests.
     */
    List<MessageView> shutdown() {
        closed = true;

        fetchExecutor.shutdownNow();
        try {
            fetchExecutor.awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        scheduler.shutdownNow();

        // Cancel any pending batch requests that haven't been fulfilled.
        BatchRequest pending;
        while ((pending = requestQueue.poll()) != null) {
            pending.lock.lock();
            try {
                if (!pending.future.isDone()) {
                    addAllToOverflow(pending.messages);
                    pending.messages.clear();
                    pending.future.cancel(true);
                }
            } finally {
                pending.lock.unlock();
            }
        }

        // Drain overflow buffer.
        final List<MessageView> remaining = new ArrayList<>();
        CachedMessage cached;
        while ((cached = overflowBuffer.poll()) != null) {
            remaining.add(cached.message);
        }
        return remaining;
    }

    // -------------------------------------------------------------------------
    // Fetch loop internals
    // -------------------------------------------------------------------------

    private void fetchLoop() {
        while (!closed) {
            BatchRequest request;
            try {
                // Block here when no batchReceive is pending (demand-driven).
                request = requestQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            try {
                fillBatch(request);
            } catch (Throwable t) {
                request.lock.lock();
                try {
                    if (!request.future.isDone()) {
                        addAllToOverflow(request.messages);
                        request.messages.clear();
                        request.future.completeExceptionally(t);
                    }
                } finally {
                    request.lock.unlock();
                }
            }
        }
        log.info("BatchFetchWorker fetch loop exited");
    }

    /**
     * Fills a single batch request.  First drains overflow, then fetches from the server in a
     * loop until the batch is full or times out.
     *
     * <p>Phase 2 fires async receives for <strong>all subscribed topics concurrently</strong>.
     * Each topic's callback directly processes messages into the request as soon as it arrives,
     * so a slow topic never blocks the processing of faster ones.
     */
    @SuppressWarnings("NullableProblems")
    private void fillBatch(BatchRequest request) {
        final AtomicReference<ScheduledFuture<?>> timeoutTaskRef = new AtomicReference<>();
        try {
            // --- Phase 1: drain overflow ---
            timeoutTaskRef.set(drainOverflowInto(request));
            if (request.future.isDone()) {
                return;
            }

            // --- Phase 2: concurrent fetch from server, results processed in-place ---
            while (!closed && !request.future.isDone()) {
                final Duration effectiveInvisible = request.invisibleDuration
                    .plus(Duration.ofNanos(request.maxWaitNanos));
                final Set<String> topics = delegate.getSubscriptionExpressions().keySet();
                if (topics.isEmpty()) {
                    break;
                }

                final List<CompletableFuture<Void>> futures = new ArrayList<>(topics.size());
                for (String topic : topics) {
                    CompletableFuture<Void> f = delegate
                        .receiveAsync(topic, FETCH_BATCH_SIZE, effectiveInvisible)
                        .thenAccept(received -> {
                            if (received == null || received.isEmpty()) {
                                return;
                            }
                            processReceivedMessages(request, received, timeoutTaskRef);
                        })
                        .exceptionally(e -> {
                            log.warn("Fetch failed for topic {}, skipping", topic, e);
                            return null;
                        });
                    futures.add(f);
                }

                try {
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (java.util.concurrent.ExecutionException e) {
                    log.warn("Unexpected error during concurrent fetch", e);
                }
            }
        } finally {
            ScheduledFuture<?> tf = timeoutTaskRef.get();
            if (tf != null) {
                tf.cancel(false);
            }
            // If still incomplete (shutdown / interrupt), move messages to overflow for cleanup.
            request.lock.lock();
            try {
                if (!request.future.isDone()) {
                    addAllToOverflow(request.messages);
                    request.messages.clear();
                    request.future.cancel(true);
                }
            } finally {
                request.lock.unlock();
            }
        }
    }

    /**
     * Processes a batch of received messages directly into the request.  Called from async
     * callbacks, so multiple invocations may run concurrently — all access is guarded by
     * {@code request.lock}.
     */
    private void processReceivedMessages(BatchRequest request, List<MessageView> received,
        AtomicReference<ScheduledFuture<?>> timeoutTaskRef) {
        request.lock.lock();
        try {
            if (request.future.isDone()) {
                addAllToOverflow(received);
                return;
            }
            for (int i = 0; i < received.size(); i++) {
                final MessageView msg = received.get(i);
                request.messages.add(msg);
                request.currentBytes += msg.getBody().remaining();

                // First message: start the deadline clock.
                if (request.messages.size() == 1 && timeoutTaskRef.get() == null) {
                    request.deadlineNanos = System.nanoTime() + request.maxWaitNanos;
                    timeoutTaskRef.set(scheduleTimeout(request));
                }

                // Batch full (count or bytes): complete immediately.
                if (request.isFull()) {
                    ScheduledFuture<?> tf = timeoutTaskRef.get();
                    if (tf != null) {
                        tf.cancel(false);
                    }
                    request.future.complete(new ArrayList<>(request.messages));
                    // Overflow any remaining messages from this receive.
                    for (int j = i + 1; j < received.size(); j++) {
                        addToOverflow(received.get(j));
                    }
                    return;
                }
            }
        } finally {
            request.lock.unlock();
        }
    }

    /**
     * Drains the overflow buffer into the request.  If the batch is immediately satisfied,
     * completes the future and returns {@code null}.  Otherwise, if at least one message was
     * drained, schedules a timeout and returns the {@link ScheduledFuture}.
     */
    private ScheduledFuture<?> drainOverflowInto(BatchRequest request) {
        request.lock.lock();
        try {
            CachedMessage cached;
            while ((cached = overflowBuffer.poll()) != null) {
                request.messages.add(cached.message);
                request.currentBytes += cached.message.getBody().remaining();
                if (request.messages.size() == 1) {
                    request.deadlineNanos = System.nanoTime() + request.maxWaitNanos;
                }
                if (request.isFull()) {
                    request.future.complete(new ArrayList<>(request.messages));
                    return null;
                }
            }
            if (!request.messages.isEmpty()) {
                return scheduleTimeout(request);
            }
        } finally {
            request.lock.unlock();
        }
        return null;
    }

    /**
     * Schedules a task that completes the batch when {@code maxWaitTime} elapses.
     * Must be called while holding {@code request.lock} or when it is guaranteed that no other
     * thread is mutating the request.
     */
    private ScheduledFuture<?> scheduleTimeout(BatchRequest request) {
        long delayNanos = request.deadlineNanos - System.nanoTime();
        return scheduler.schedule(() -> {
            request.lock.lock();
            try {
                if (!request.future.isDone() && !request.messages.isEmpty()) {
                    request.future.complete(new ArrayList<>(request.messages));
                }
            } finally {
                request.lock.unlock();
            }
        }, Math.max(0, delayNanos), TimeUnit.NANOSECONDS);
    }

    // -------------------------------------------------------------------------
    // Cache eviction
    // -------------------------------------------------------------------------

    /**
     * Evicts messages from the overflow buffer that have been idle longer than
     * {@code cacheEvictionTime}.  Since messages are appended in chronological order,
     * we only need to scan from the head until we find a non-expired entry.
     */
    private void evictExpiredCache() {
        if (closed) {
            return;
        }
        final long now = System.currentTimeMillis();
        final long evictionMillis = cacheEvictionTime.toMillis();
        CachedMessage head;
        while ((head = overflowBuffer.peekFirst()) != null) {
            if (now - head.cachedAtMillis < evictionMillis) {
                break;
            }
            CachedMessage evicted = overflowBuffer.pollFirst();
            if (evicted == null) {
                break;
            }
            try {
                log.info("Evicted cached message due to inactivity, messageId={}, cachedAt={}",
                    evicted.message.getMessageId(), evicted.cachedAtMillis);
                delegate.changeInvisibleDurationAsync(evicted.message, RELEASE_INVISIBLE_DURATION, true)
                    .whenComplete((v, t) -> {
                        if (t != null) {
                            log.warn("Failed to release evicted message, messageId={}",
                                evicted.message.getMessageId(), t);
                        } else {
                            log.info("Released evicted message successfully, messageId={}",
                                evicted.message.getMessageId());
                        }
                });
            } catch (Throwable t) {
                log.warn("Failed to release evicted message, messageId={}",
                    evicted.message.getMessageId(), t);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Topic eviction
    // -------------------------------------------------------------------------

    /**
     * Removes all cached messages belonging to the specified topic from the overflow buffer and
     * releases them back to the server with {@code suspend=true}.
     *
     * <p>Called when a topic is unsubscribed so that stale messages do not linger in cache.
     */
    void evictTopic(String topic) {
        overflowBuffer.removeIf(cached -> {
            if (topic.equals(cached.message.getTopic())) {
                try {
                    delegate.changeInvisibleDurationAsync(cached.message, RELEASE_INVISIBLE_DURATION, true)
                        .whenComplete((v, t) -> {
                            if (t != null) {
                                log.warn("Failed to release message on topic eviction, messageId={}",
                                    cached.message.getMessageId(), t);
                            }
                        });
                } catch (Throwable t) {
                    log.warn("Failed to release message on topic eviction, messageId={}",
                        cached.message.getMessageId(), t);
                }
                return true;
            }
            return false;
        });
    }

    // -------------------------------------------------------------------------
    // Overflow helpers
    // -------------------------------------------------------------------------

    private void addToOverflow(MessageView message) {
        overflowBuffer.addLast(new CachedMessage(message));
    }

    private void addAllToOverflow(List<MessageView> messages) {
        for (MessageView msg : messages) {
            overflowBuffer.addLast(new CachedMessage(msg));
        }
    }

    // -------------------------------------------------------------------------
    // Inner class
    // -------------------------------------------------------------------------

    /**
     * Wraps a {@link MessageView} with the timestamp when it was placed into the overflow buffer.
     */
    static class CachedMessage {
        final MessageView message;
        final long cachedAtMillis;

        CachedMessage(MessageView message) {
            this.message = message;
            this.cachedAtMillis = System.currentTimeMillis();
        }
    }
}
