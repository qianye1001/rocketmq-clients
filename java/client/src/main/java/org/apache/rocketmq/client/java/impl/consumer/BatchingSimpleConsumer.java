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
 * A {@link SimpleConsumer} decorator that adds client-side message buffering to support
 * {@link #batchReceive(Duration)} and {@link #batchReceiveAsync(Duration)} semantics.
 *
 * <h3>Design</h3>
 * <ul>
 *   <li><strong>Demand-driven fetching</strong>: the {@link BatchFetchWorker} is idle when there
 *       are no pending {@code batchReceive} requests.  It only calls
 *       {@link SimpleConsumer#receive(int, Duration)} on the delegate when at least one request
 *       is queued, avoiding unnecessary server load.</li>
 *   <li><strong>FIFO request ordering</strong>: multiple concurrent {@code batchReceive} /
 *       {@code batchReceiveAsync} calls are queued and fulfilled strictly in the order they
 *       were submitted.</li>
 *   <li><strong>No polling loops in the caller</strong>: {@code batchReceive} blocks on a
 *       {@link CompletableFuture#get()} and is woken up by the worker via
 *       {@link CompletableFuture#complete(Object)} or a scheduled timeout task.</li>
 *   <li><strong>Graceful shutdown</strong>: on {@link #close()}, any messages still buffered
 *       internally have their invisible duration shortened to 1 second via
 *       {@link SimpleConsumer#changeInvisibleDuration(MessageView, Duration)} so that they
 *       become visible to other consumers promptly.</li>
 *   <li>{@link #batchAck(List)} delegates directly to the underlying consumer's true
 *       batch-ack implementation, which sends all entries in a single network round-trip.</li>
 * </ul>
 *
 * <h3>Thread safety</h3>
 * {@link #batchReceive(Duration)} and {@link #batchReceiveAsync(Duration)} are safe to call from
 * multiple threads.  Requests are enqueued atomically and processed by a single worker thread.
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
        this.worker = new BatchFetchWorker(delegate, batchPolicy);
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
     * <p>Delegates to the underlying consumer's true batch-ack, sending all entries in a single
     * network round-trip per (endpoints, topic) group.
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
        return this;
    }

    @Override
    public Map<String, FilterExpression> getSubscriptionExpressions() {
        return delegate.getSubscriptionExpressions();
    }

    @Override
    public List<MessageView> receive(int maxMessageNum, Duration invisibleDuration)
        throws ClientException {
        return delegate.receive(maxMessageNum, invisibleDuration);
    }

    @Override
    public CompletableFuture<List<MessageView>> receiveAsync(int maxMessageNum,
        Duration invisibleDuration) {
        return delegate.receiveAsync(maxMessageNum, invisibleDuration);
    }

    @Override
    public void ack(MessageView messageView) throws ClientException {
        delegate.ack(messageView);
    }

    @Override
    public CompletableFuture<Void> ackAsync(MessageView messageView) {
        return delegate.ackAsync(messageView);
    }

    @Override
    public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration)
        throws ClientException {
        delegate.changeInvisibleDuration(messageView, invisibleDuration);
    }

    @Override
    public CompletableFuture<Void> changeInvisibleDurationAsync(MessageView messageView,
        Duration invisibleDuration) {
        return delegate.changeInvisibleDurationAsync(messageView, invisibleDuration);
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
                delegate.changeInvisibleDuration(m, RELEASE_INVISIBLE_DURATION);
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
