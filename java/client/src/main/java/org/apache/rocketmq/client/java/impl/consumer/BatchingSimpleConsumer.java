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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SimpleConsumer} decorator that adds client-side message buffering.
 *
 * <p>Both {@link #receive(int, Duration)} and {@link #batchReceive(Duration)} go through the same
 * {@link BatchFetchWorker}, sharing a single overflow buffer.  The difference in behavior is
 * purely determined by the {@link BatchRequest} parameters:
 * <ul>
 *   <li><strong>{@code receive(n, duration)}</strong>: submits a request with
 *       {@code maxBatchSize=n, maxWaitNanos=0} — returns immediately from cache or after a
 *       single server fetch, surplus messages are buffered for subsequent calls.</li>
 *   <li><strong>{@code batchReceive(duration)}</strong>: submits a request with
 *       {@code maxBatchSize/maxBatchBytes/maxWaitNanos} from {@link BatchPolicy} — accumulates
 *       until the batch is full or the timeout fires.</li>
 * </ul>
 *
 * <h3>Design</h3>
 * <ul>
 *   <li><strong>Demand-driven fetching</strong>: the {@link BatchFetchWorker} is idle when there
 *       are no pending requests.  It only calls {@link SimpleConsumer#receive(int, Duration)} on
 *       the delegate when at least one request is queued.</li>
 *   <li><strong>FIFO request ordering</strong>: all calls are queued and fulfilled strictly in
 *       the order they were submitted.</li>
 *   <li><strong>Unified cache</strong>: overflow from any receive call is available to
 *       subsequent calls, regardless of whether they are batch or non-batch.</li>
 *   <li><strong>Graceful shutdown</strong>: on {@link #close()}, any messages still buffered
 *       internally have their invisible duration shortened to 1 second so that they become
 *       visible to other consumers promptly.</li>
 * </ul>
 *
 * <h3>Thread safety</h3>
 * All receive methods are safe to call from multiple threads.  Requests are enqueued atomically
 * and processed by a single worker thread.
 *
 * @see BatchFetchWorker
 * @see BatchRequest
 */
public class BatchingSimpleConsumer implements SimpleConsumer {

    private static final Logger log = LoggerFactory.getLogger(BatchingSimpleConsumer.class);

    private static final Duration RELEASE_INVISIBLE_DURATION = Duration.ofSeconds(1);

    private final SimpleConsumer delegate;
    private final BatchPolicy batchPolicy;
    private final BatchFetchWorker worker;

    private BatchingSimpleConsumer(SimpleConsumer delegate, BatchPolicy batchPolicy) {
        checkNotNull(delegate, "delegate should not be null");
        checkNotNull(batchPolicy, "batchPolicy should not be null");
        this.delegate = delegate;
        this.batchPolicy = batchPolicy;
        this.worker = new BatchFetchWorker(delegate, batchPolicy.getCacheEvictionTime());
    }

    /**
     * Creates and starts a new {@link BatchingSimpleConsumer}.
     *
     * <p>The background worker is started after construction to satisfy SpotBugs
     * {@code SC_START_IN_CTOR}.
     */
    public static BatchingSimpleConsumer create(SimpleConsumer delegate, BatchPolicy batchPolicy) {
        BatchingSimpleConsumer consumer = new BatchingSimpleConsumer(delegate, batchPolicy);
        consumer.worker.start();
        return consumer;
    }

    // -------------------------------------------------------------------------
    // Batch API
    // -------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     *
     * <p>Enqueues a batch request and <strong>blocks</strong> until the background worker
     * fulfills it.  The caller is woken by the worker completing the underlying
     * {@link CompletableFuture}.
     */
    @Override
    public List<MessageView> batchReceive(Duration invisibleDuration) throws ClientException {
        final CompletableFuture<List<MessageView>> future = batchReceiveAsync(invisibleDuration);
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClientException("Interrupted while waiting for batch receive");
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof ClientException) {
                throw (ClientException) cause;
            }
            throw new ClientException("Batch receive failed", cause);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Enqueues a batch request and returns a future immediately.  Multiple calls are
     * fulfilled strictly in FIFO order.
     */
    @Override
    public CompletableFuture<List<MessageView>> batchReceiveAsync(Duration invisibleDuration) {
        checkNotNull(invisibleDuration, "invisibleDuration should not be null");
        final BatchRequest request = new BatchRequest(
            batchPolicy.getMaxBatchSize(),
            batchPolicy.getMaxBatchBytes(),
            batchPolicy.getMaxWaitTime().toNanos(),
            invisibleDuration
        );
        worker.submit(request);
        return request.future;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Delegates to the underlying consumer's batch-ack, which groups entries by
     * (endpoints, topic) and splits each group into bounded chunks before sending RPCs.
     */
    @Override
    public void batchAck(List<MessageView> messageViews) throws ClientException {
        delegate.batchAck(messageViews);
    }

    @Override
    public CompletableFuture<Void> batchAckAsync(List<MessageView> messageViews) {
        return delegate.batchAckAsync(messageViews);
    }

    // -------------------------------------------------------------------------
    // Delegate methods
    // -------------------------------------------------------------------------

    @Override
    public String getConsumerGroup() {
        return delegate.getConsumerGroup();
    }

    @Override
    public SimpleConsumer subscribe(String topic, FilterExpression filterExpression)
        throws ClientException {
        delegate.subscribe(topic, filterExpression);
        return this;
    }

    @Override
    public SimpleConsumer unsubscribe(String topic) throws ClientException {
        delegate.unsubscribe(topic);
        worker.evictTopic(topic);
        return this;
    }

    @Override
    public Map<String, FilterExpression> getSubscriptionExpressions() {
        return delegate.getSubscriptionExpressions();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Submits a request with {@code maxWaitNanos=0} to the shared worker.  If the overflow
     * buffer has cached messages, they are returned immediately without hitting the server.
     * Otherwise, the worker fetches from the server and returns up to {@code maxMessageNum}
     * messages, caching any surplus for subsequent calls.
     */
    @Override
    public List<MessageView> receive(int maxMessageNum, Duration invisibleDuration)
        throws ClientException {
        final CompletableFuture<List<MessageView>> future = receiveAsync(maxMessageNum,
            invisibleDuration);
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClientException("Interrupted while waiting for receive");
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof ClientException) {
                throw (ClientException) cause;
            }
            throw new ClientException("Receive failed", cause);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Submits a request with {@code maxWaitNanos=0} to the shared worker and returns a
     * future immediately.  The same overflow buffer is shared with batch requests.
     */
    @Override
    public CompletableFuture<List<MessageView>> receiveAsync(int maxMessageNum,
        Duration invisibleDuration) {
        checkNotNull(invisibleDuration, "invisibleDuration should not be null");
        final BatchRequest request = new BatchRequest(
            maxMessageNum, Long.MAX_VALUE, 0L, invisibleDuration);
        worker.submit(request);
        return request.future;
    }

    @Override
    public void ack(MessageView messageView) throws ClientException {
        delegate.ack(messageView);
    }

    @Override
    public CompletableFuture<Void> ackAsync(MessageView messageView) {
        return delegate.ackAsync(messageView);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Topic-specific receive delegates directly to the underlying consumer, bypassing
     * the shared overflow buffer.  This gives the caller precise control over which
     * topic to poll, avoiding head-of-line blocking caused by empty topics.
     */
    @Override
    public List<MessageView> receive(String topic, int maxMessageNum, Duration invisibleDuration)
        throws ClientException {
        return delegate.receive(topic, maxMessageNum, invisibleDuration);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Async variant of topic-specific receive; delegates directly to the underlying consumer.
     */
    @Override
    public CompletableFuture<List<MessageView>> receiveAsync(String topic, int maxMessageNum,
        Duration invisibleDuration) {
        return delegate.receiveAsync(topic, maxMessageNum, invisibleDuration);
    }

    @Override
    public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration)
        throws ClientException {
        delegate.changeInvisibleDuration(messageView, invisibleDuration);
    }

    @Override
    public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration, boolean suspend)
        throws ClientException {
        delegate.changeInvisibleDuration(messageView, invisibleDuration, suspend);
    }

    @Override
    public CompletableFuture<Void> changeInvisibleDurationAsync(MessageView messageView,
        Duration invisibleDuration) {
        return delegate.changeInvisibleDurationAsync(messageView, invisibleDuration);
    }

    @Override
    public CompletableFuture<Void> changeInvisibleDurationAsync(MessageView messageView,
        Duration invisibleDuration, boolean suspend) {
        return delegate.changeInvisibleDurationAsync(messageView, invisibleDuration, suspend);
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() throws IOException {
        // 1. Shut down the worker and collect any messages still buffered internally.
        final List<MessageView> remaining = worker.shutdown();

        // 2. Release all buffered messages by shortening their invisible duration to 1 second
        //    so that the server makes them visible to other consumers quickly.
        for (MessageView m : remaining) {
            try {
                delegate.changeInvisibleDuration(m, RELEASE_INVISIBLE_DURATION, true);
                log.info("Released buffered message on close, messageId={}", m.getMessageId());
            } catch (Throwable t) {
                log.warn("Failed to shorten invisible duration for buffered message, messageId={}",
                    m.getMessageId(), t);
            }
        }

        // 3. Close the underlying consumer.
        delegate.close();
    }
}
