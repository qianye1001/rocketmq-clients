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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.client.apis.message.MessageView;

/**
 * Holds the state for a single {@code batchReceive} / {@code batchReceiveAsync} request.
 *
 * <p>Instances are created by the caller thread and consumed by the background
 * {@link BatchFetchWorker}.  Mutable fields ({@link #messages}, {@link #currentBytes},
 * {@link #deadlineNanos}) are protected by {@link #lock}.
 */
final class BatchRequest {

    final CompletableFuture<List<MessageView>> future = new CompletableFuture<>();

    /**
     * Lock protecting mutable state.  Uses {@link ReentrantLock} instead of
     * {@code synchronized} to avoid pinning virtual threads to carrier threads.
     */
    final Lock lock = new ReentrantLock();

    /** Guarded by {@link #lock}. */
    final List<MessageView> messages = new ArrayList<>();

    final int maxBatchSize;
    final long maxBatchBytes;
    final long maxWaitNanos;

    /**
     * The invisible duration requested by the caller.  The background worker adds
     * {@code batchPolicy.maxWaitTime} on top when calling the delegate, so that messages
     * remain invisible long enough to cover the batch-aggregation window.
     */
    final Duration invisibleDuration;

    /** Accumulated body size (bytes) of messages in {@link #messages}.  Guarded by {@link #lock}. */
    long currentBytes;

    /** Nanotime deadline set when the first message is added.  0 means "not started". */
    long deadlineNanos;

    BatchRequest(int maxBatchSize, long maxBatchBytes, long maxWaitNanos,
        Duration invisibleDuration) {
        this.maxBatchSize = maxBatchSize;
        this.maxBatchBytes = maxBatchBytes;
        this.maxWaitNanos = maxWaitNanos;
        this.invisibleDuration = invisibleDuration;
    }

    /** Whether the batch has reached its capacity (count or bytes). */
    boolean isFull() {
        return messages.size() >= maxBatchSize || currentBytes >= maxBatchBytes;
    }
}
