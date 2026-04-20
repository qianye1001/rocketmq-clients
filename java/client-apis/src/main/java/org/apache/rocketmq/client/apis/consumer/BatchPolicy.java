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

    /**
     * Default time that cached messages may remain idle in the overflow buffer before being
     * released back to the server.  If no request consumes them within this window, their
     * invisible duration is shortened so they become visible to other consumers.
     */
    public static final Duration DEFAULT_CACHE_EVICTION_TIME = Duration.ofMinutes(5);

    private final int maxBatchSize;
    private final long maxBatchBytes;
    private final Duration maxWaitTime;
    private final Duration cacheEvictionTime;

    /**
     * Creates a {@link BatchPolicy} with the specified message count and wait time,
     * using the {@link #DEFAULT_MAX_BATCH_BYTES default byte limit} and
     * {@link #DEFAULT_CACHE_EVICTION_TIME default eviction time}.
     *
     * @param maxBatchSize the maximum number of messages in a batch; must be &gt; 0.
     * @param maxWaitTime  the maximum time to wait for the batch to fill up; must be positive.
     */
    public BatchPolicy(int maxBatchSize, Duration maxWaitTime) {
        this(maxBatchSize, DEFAULT_MAX_BATCH_BYTES, maxWaitTime, DEFAULT_CACHE_EVICTION_TIME);
    }

    /**
     * Creates a {@link BatchPolicy} with the specified parameters and
     * {@link #DEFAULT_CACHE_EVICTION_TIME default eviction time}.
     *
     * @param maxBatchSize  the maximum number of messages in a batch; must be &gt; 0.
     * @param maxBatchBytes the maximum total body size (in bytes) of messages in a batch; must be &gt; 0.
     *                      This is critical for preventing client-side OOM when messages have large bodies.
     * @param maxWaitTime   the maximum time to wait for the batch to fill up; must be positive.
     */
    public BatchPolicy(int maxBatchSize, long maxBatchBytes, Duration maxWaitTime) {
        this(maxBatchSize, maxBatchBytes, maxWaitTime, DEFAULT_CACHE_EVICTION_TIME);
    }

    /**
     * Creates a {@link BatchPolicy} with all parameters specified.
     *
     * @param maxBatchSize      the maximum number of messages in a batch; must be &gt; 0.
     * @param maxBatchBytes     the maximum total body size (in bytes) of messages in a batch; must be &gt; 0.
     * @param maxWaitTime       the maximum time to wait for the batch to fill up; must be positive.
     * @param cacheEvictionTime the maximum time that cached messages may remain idle in the
     *                          overflow buffer before being released back to the server; must be positive.
     */
    public BatchPolicy(int maxBatchSize, long maxBatchBytes, Duration maxWaitTime,
        Duration cacheEvictionTime) {
        checkArgument(maxBatchSize > 0, "maxBatchSize must be greater than 0");
        checkArgument(maxBatchBytes > 0, "maxBatchBytes must be greater than 0");
        checkNotNull(maxWaitTime, "maxWaitTime should not be null");
        checkArgument(!maxWaitTime.isNegative() && !maxWaitTime.isZero(),
            "maxWaitTime must be positive");
        checkNotNull(cacheEvictionTime, "cacheEvictionTime should not be null");
        checkArgument(!cacheEvictionTime.isNegative() && !cacheEvictionTime.isZero(),
            "cacheEvictionTime must be positive");
        this.maxBatchSize = maxBatchSize;
        this.maxBatchBytes = maxBatchBytes;
        this.maxWaitTime = maxWaitTime;
        this.cacheEvictionTime = cacheEvictionTime;
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

    /**
     * Returns the maximum time that cached messages may remain idle in the overflow buffer
     * before being released back to the server via
     * {@link SimpleConsumer#changeInvisibleDuration(org.apache.rocketmq.client.apis.message.MessageView, Duration)}.
     *
     * <p>When no {@code receive} or {@code batchReceive} call consumes the cached messages within
     * this window, they are proactively released so that other consumers can process them.
     *
     * @return cache eviction time.
     */
    public Duration getCacheEvictionTime() {
        return cacheEvictionTime;
    }

    /**
     * Returns a new {@link Builder} with all defaults pre-populated.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "BatchPolicy{maxBatchSize=" + maxBatchSize
            + ", maxBatchBytes=" + maxBatchBytes
            + ", maxWaitTime=" + maxWaitTime
            + ", cacheEvictionTime=" + cacheEvictionTime + '}';
    }

    /**
     * Builder for {@link BatchPolicy}.  All fields have sensible defaults; only override
     * what you need.
     *
     * <pre>{@code
     * BatchPolicy policy = BatchPolicy.builder()
     *     .setMaxBatchSize(64)
     *     .setMaxWaitTime(Duration.ofSeconds(10))
     *     .build();
     * }</pre>
     */
    public static class Builder {
        private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
        private long maxBatchBytes = DEFAULT_MAX_BATCH_BYTES;
        private Duration maxWaitTime = DEFAULT_MAX_WAIT_TIME;
        private Duration cacheEvictionTime = DEFAULT_CACHE_EVICTION_TIME;

        Builder() {
        }

        /**
         * Sets the maximum number of messages in a batch.
         *
         * @param maxBatchSize must be &gt; 0; default {@value DEFAULT_MAX_BATCH_SIZE}.
         */
        public Builder setMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        /**
         * Sets the maximum total body size (in bytes) before a batch is returned.
         *
         * @param maxBatchBytes must be &gt; 0; default {@value DEFAULT_MAX_BATCH_BYTES}.
         */
        public Builder setMaxBatchBytes(long maxBatchBytes) {
            this.maxBatchBytes = maxBatchBytes;
            return this;
        }

        /**
         * Sets the maximum time to wait for the batch to fill up.
         *
         * @param maxWaitTime must be positive; default 5 seconds.
         */
        public Builder setMaxWaitTime(Duration maxWaitTime) {
            this.maxWaitTime = maxWaitTime;
            return this;
        }

        /**
         * Sets the maximum time that cached messages may remain idle in the overflow buffer
         * before being released back to the server.
         *
         * @param cacheEvictionTime must be positive; default 5 minutes.
         */
        public Builder setCacheEvictionTime(Duration cacheEvictionTime) {
            this.cacheEvictionTime = cacheEvictionTime;
            return this;
        }

        /**
         * Builds a new {@link BatchPolicy} with the configured values.
         *
         * @return a new {@link BatchPolicy} instance.
         * @throws IllegalArgumentException if any parameter is invalid.
         */
        public BatchPolicy build() {
            return new BatchPolicy(maxBatchSize, maxBatchBytes, maxWaitTime, cacheEvictionTime);
        }
    }
}
