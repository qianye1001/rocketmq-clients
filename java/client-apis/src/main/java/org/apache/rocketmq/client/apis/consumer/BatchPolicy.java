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

package org.apache.rocketmq.client.apis.consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Duration;

/**
 * Policy that controls how {@link SimpleConsumer#batchReceive(Duration)} aggregates messages from the local cache
 * before returning a batch to the caller.
 *
 * <p>A batch is considered ready when <em>any</em> of the following conditions is met:
 * <ol>
 *   <li>The number of buffered messages reaches {@code maxBatchSize}.</li>
 *   <li>The total body size (in bytes) of buffered messages reaches {@code maxBatchBytes}.</li>
 *   <li>The time elapsed since the first message was buffered reaches {@code maxWaitTime}.</li>
 * </ol>
 *
 * <p>The {@code maxBatchBytes} limit is critical for preventing client-side OOM when messages
 * have large bodies.
 *
 * <p>Note: the actual server‑side invisible duration is
 * {@code invisibleDuration} (passed to {@link SimpleConsumer#batchReceive(Duration)}) plus
 * {@code maxWaitTime}, so callers must take this into account when choosing {@code invisibleDuration}.
 */
public class BatchPolicy {

    /**
     * Default maximum number of messages returned in a single batch.
     */
    public static final int DEFAULT_MAX_BATCH_SIZE = 32;

    /**
     * Default maximum total body size (in bytes) before a batch is returned.
     * 4 MB by default.
     */
    public static final long DEFAULT_MAX_BATCH_BYTES = 4L * 1024 * 1024;

    /**
     * Default maximum wait time before a partial batch is returned.
     */
    public static final Duration DEFAULT_MAX_WAIT_TIME = Duration.ofSeconds(5);

    private final int maxBatchSize;
    private final long maxBatchBytes;
    private final Duration maxWaitTime;

    /**
     * Creates a {@link BatchPolicy} with the specified message count and wait time,
     * using the {@link #DEFAULT_MAX_BATCH_BYTES default byte limit}.
     *
     * @param maxBatchSize the maximum number of messages in a batch; must be &gt; 0.
     * @param maxWaitTime  the maximum time to wait for the batch to fill up; must be positive.
     */
    public BatchPolicy(int maxBatchSize, Duration maxWaitTime) {
        this(maxBatchSize, DEFAULT_MAX_BATCH_BYTES, maxWaitTime);
    }

    /**
     * Creates a {@link BatchPolicy} with the specified parameters.
     *
     * @param maxBatchSize  the maximum number of messages in a batch; must be &gt; 0.
     * @param maxBatchBytes the maximum total body size (in bytes) of messages in a batch; must be &gt; 0.
     *                      This is critical for preventing client-side OOM when messages have large bodies.
     * @param maxWaitTime   the maximum time to wait for the batch to fill up; must be positive.
     */
    public BatchPolicy(int maxBatchSize, long maxBatchBytes, Duration maxWaitTime) {
        checkArgument(maxBatchSize > 0, "maxBatchSize must be greater than 0");
        checkArgument(maxBatchBytes > 0, "maxBatchBytes must be greater than 0");
        checkNotNull(maxWaitTime, "maxWaitTime should not be null");
        checkArgument(!maxWaitTime.isNegative() && !maxWaitTime.isZero(),
            "maxWaitTime must be positive");
        this.maxBatchSize = maxBatchSize;
        this.maxBatchBytes = maxBatchBytes;
        this.maxWaitTime = maxWaitTime;
    }

    /**
     * Returns the maximum number of messages that can be contained in a single batch.
     *
     * @return max batch size.
     */
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    /**
     * Returns the maximum total body size (in bytes) of messages in a single batch.
     * When the accumulated body size of buffered messages reaches this limit, the batch is returned
     * to the caller immediately.
     *
     * @return max batch bytes.
     */
    public long getMaxBatchBytes() {
        return maxBatchBytes;
    }

    /**
     * Returns the maximum time to wait before returning a partial batch.
     *
     * @return max wait time.
     */
    public Duration getMaxWaitTime() {
        return maxWaitTime;
    }

    @Override
    public String toString() {
        return "BatchPolicy{maxBatchSize=" + maxBatchSize
            + ", maxBatchBytes=" + maxBatchBytes
            + ", maxWaitTime=" + maxWaitTime + '}';
    }
}
