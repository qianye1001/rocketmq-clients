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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.apis.ClientException;
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
 * from overflow or after a single fetch) — the difference is entirely determined by the
 * {@link BatchRequest} parameters ({@code maxWaitNanos = 0} means no accumulation wait).
 *
 * <p><strong>Demand-driven</strong>: the worker blocks on {@link BlockingQueue#take()} when no
 * requests are pending, producing zero server traffic.
 */
final class BatchFetchWorker {

    private static final Logger log = LoggerFactory.getLogger(BatchFetchWorker.class);

    /**
     * Number of messages the background worker attempts to pull per {@code receive()} call.
     */
    private static final int FETCH_BATCH_SIZE = 32;

    private final SimpleConsumer delegate;

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
     */
    private final ConcurrentLinkedDeque<MessageView> overflowBuffer = new ConcurrentLinkedDeque<>();

    private volatile boolean closed = false;

    BatchFetchWorker(SimpleConsumer delegate) {
        this.delegate = delegate;
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
            fetchExecutor.awaitTermination(5, TimeUnit.SECONDS);
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
                    overflowBuffer.addAll(pending.messages);
                    pending.messages.clear();
                    pending.future.cancel(true);
                }
            } finally {
                pending.lock.unlock();
            }
        }

        // Drain overflow buffer.
        final List<MessageView> remaining = new ArrayList<>();
        MessageView msg;
        while ((msg = overflowBuffer.poll()) != null) {
            remaining.add(msg);
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
                        overflowBuffer.addAll(request.messages);
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
     */
    @SuppressWarnings("NullableProblems")
    private void fillBatch(BatchRequest request) {
        ScheduledFuture<?> timeoutTask = null;
        try {
            // --- Phase 1: drain overflow ---
            timeoutTask = drainOverflowInto(request);
            if (request.future.isDone()) {
                return;
            }

            // --- Phase 2: fetch from server ---
            while (!closed && !request.future.isDone()) {
                List<MessageView> received;
                try {
                    // Compensate invisible duration by the request's max wait time so that
                    // messages remain invisible long enough to cover the accumulation window.
                    // For non-batch requests (maxWaitNanos=0), no compensation is added.
                    Duration effectiveInvisible = request.invisibleDuration
                        .plus(Duration.ofNanos(request.maxWaitNanos));
                    received = delegate.receive(FETCH_BATCH_SIZE, effectiveInvisible);
                } catch (ClientException e) {
                    if (closed || Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    log.warn("Background fetch failed, will retry", e);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    continue;
                }

                request.lock.lock();
                try {
                    if (request.future.isDone()) {
                        // Timeout fired while we were in receive().
                        overflowBuffer.addAll(received);
                        break;
                    }
                    for (int i = 0; i < received.size(); i++) {
                        final MessageView msg = received.get(i);
                        request.messages.add(msg);
                        request.currentBytes += msg.getBody().remaining();

                        // First message: start the deadline clock.
                        if (request.messages.size() == 1 && timeoutTask == null) {
                            request.deadlineNanos = System.nanoTime() + request.maxWaitNanos;
                            timeoutTask = scheduleTimeout(request);
                        }

                        // Batch full (count or bytes): complete immediately.
                        if (request.isFull()) {
                            if (timeoutTask != null) {
                                timeoutTask.cancel(false);
                            }
                            request.future.complete(new ArrayList<>(request.messages));
                            // Overflow any remaining messages.
                            for (int j = i + 1; j < received.size(); j++) {
                                overflowBuffer.addLast(received.get(j));
                            }
                            return;
                        }
                    }
                } finally {
                    request.lock.unlock();
                }
            }
        } finally {
            if (timeoutTask != null) {
                timeoutTask.cancel(false);
            }
            // If still incomplete (shutdown / interrupt), move messages to overflow for cleanup.
            request.lock.lock();
            try {
                if (!request.future.isDone()) {
                    overflowBuffer.addAll(request.messages);
                    request.messages.clear();
                    request.future.cancel(true);
                }
            } finally {
                request.lock.unlock();
            }
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
            MessageView msg;
            while ((msg = overflowBuffer.poll()) != null) {
                request.messages.add(msg);
                request.currentBytes += msg.getBody().remaining();
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
}
