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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.BatchPolicy;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class BatchFetchWorkerTest extends TestBase {

    private static final Duration INVISIBLE_DURATION = Duration.ofSeconds(10);
    private static final Duration DEFAULT_EVICTION = Duration.ofMinutes(5);

    @Mock
    private SimpleConsumer delegate;

    private BatchFetchWorker worker;

    @After
    public void tearDown() {
        if (worker != null) {
            worker.shutdown();
            worker = null;
        }
    }

    private BatchFetchWorker createAndStartWorker() {
        return createAndStartWorker(DEFAULT_EVICTION);
    }

    private BatchFetchWorker createAndStartWorker(Duration cacheEvictionTime) {
        BatchFetchWorker w = new BatchFetchWorker(delegate, cacheEvictionTime);
        w.start();
        return w;
    }

    /**
     * Helper: create a list of fake MessageView with specified body size.
     */
    private List<MessageView> fakeMessages(int count, int bodySize) {
        List<MessageView> msgs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            msgs.add(fakeMessageViewImpl(bodySize, false));
        }
        return msgs;
    }

    // -----------------------------------------------------------------------
    // 1. Batch fulfilled by message count
    // -----------------------------------------------------------------------

    @Test
    public void testBatchCompletedByCount() throws Exception {
        final int maxBatch = 3;
        BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        worker = createAndStartWorker();

        // Delegate returns exactly maxBatch messages on first call.
        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenReturn(fakeMessages(maxBatch, 10));

        BatchRequest request = new BatchRequest(maxBatch, Long.MAX_VALUE,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        worker.submit(request);

        List<MessageView> result = request.future.get(5, TimeUnit.SECONDS);
        assertEquals(maxBatch, result.size());
    }

    // -----------------------------------------------------------------------
    // 2. Batch fulfilled by bytes limit
    // -----------------------------------------------------------------------

    @Test
    public void testBatchCompletedByBytes() throws Exception {
        final int bodySize = 1024;
        // maxBatchBytes = 2KB, each message body is 1KB, so 2 messages should trigger
        final long maxBatchBytes = 2 * 1024L;
        final int maxBatchSize = 100; // high count limit, won't be hit

        BatchPolicy policy = new BatchPolicy(maxBatchSize, maxBatchBytes, Duration.ofSeconds(5));
        worker = createAndStartWorker();

        // Return 5 messages at once; only 2 should be in the batch, rest overflows.
        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenReturn(fakeMessages(5, bodySize));

        BatchRequest request = new BatchRequest(maxBatchSize, maxBatchBytes,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        worker.submit(request);

        List<MessageView> result = request.future.get(5, TimeUnit.SECONDS);
        assertEquals(2, result.size());
    }

    // -----------------------------------------------------------------------
    // 3. Batch fulfilled by timeout (partial batch)
    // -----------------------------------------------------------------------

    @Test
    public void testBatchCompletedByTimeout() throws Exception {
        final int maxBatch = 100; // won't be hit by count
        final Duration maxWait = Duration.ofMillis(300);
        BatchPolicy policy = new BatchPolicy(maxBatch, maxWait);
        worker = createAndStartWorker();

        // First call returns 2 messages, second call blocks forever (simulate no more data).
        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenReturn(fakeMessages(2, 10))
            .thenAnswer((Answer<List<MessageView>>) invocation -> {
                // Block until interrupted
                Thread.sleep(60_000);
                return Collections.emptyList();
            });

        BatchRequest request = new BatchRequest(maxBatch, Long.MAX_VALUE,
            maxWait.toNanos(), INVISIBLE_DURATION);
        worker.submit(request);

        // Should complete within timeout + some tolerance.
        List<MessageView> result = request.future.get(5, TimeUnit.SECONDS);
        assertEquals(2, result.size());
    }

    // -----------------------------------------------------------------------
    // 4. Overflow buffer: surplus messages served to next request
    // -----------------------------------------------------------------------

    @Test
    public void testOverflowBufferFeedsNextRequest() throws Exception {
        final int maxBatch = 2;
        BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        worker = createAndStartWorker();

        // Delegate returns 5 messages on first call; only 2 needed for first request,
        // 3 go to overflow. Second request should be served from overflow without calling delegate again.
        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenReturn(fakeMessages(5, 10));

        // First request
        BatchRequest req1 = new BatchRequest(maxBatch, Long.MAX_VALUE,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        worker.submit(req1);
        List<MessageView> result1 = req1.future.get(5, TimeUnit.SECONDS);
        assertEquals(maxBatch, result1.size());

        // Second request: should be fulfilled from overflow (3 messages available, needs 2)
        BatchRequest req2 = new BatchRequest(maxBatch, Long.MAX_VALUE,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        worker.submit(req2);
        List<MessageView> result2 = req2.future.get(5, TimeUnit.SECONDS);
        assertEquals(maxBatch, result2.size());
    }

    // -----------------------------------------------------------------------
    // 5. FIFO ordering: multiple requests fulfilled in submit order
    // -----------------------------------------------------------------------

    @Test
    public void testFifoOrdering() throws Exception {
        final int maxBatch = 1;
        BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        worker = createAndStartWorker();

        // Each call returns exactly 1 message.
        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenAnswer(invocation -> fakeMessages(1, 10));

        BatchRequest req1 = new BatchRequest(maxBatch, Long.MAX_VALUE,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        BatchRequest req2 = new BatchRequest(maxBatch, Long.MAX_VALUE,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        BatchRequest req3 = new BatchRequest(maxBatch, Long.MAX_VALUE,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);

        worker.submit(req1);
        worker.submit(req2);
        worker.submit(req3);

        // All should complete; req1 completes first.
        req1.future.get(5, TimeUnit.SECONDS);
        req2.future.get(5, TimeUnit.SECONDS);
        req3.future.get(5, TimeUnit.SECONDS);

        assertTrue(req1.future.isDone());
        assertTrue(req2.future.isDone());
        assertTrue(req3.future.isDone());
    }

    // -----------------------------------------------------------------------
    // 6. Shutdown returns buffered messages
    // -----------------------------------------------------------------------

    @Test
    public void testShutdownReturnsBufferedMessages() throws Exception {
        final int maxBatch = 100; // won't be filled
        BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(30));
        worker = createAndStartWorker();

        // Delegate returns 3 messages, then blocks forever.
        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenReturn(fakeMessages(3, 10))
            .thenAnswer(invocation -> {
                Thread.sleep(60_000);
                return Collections.emptyList();
            });

        BatchRequest request = new BatchRequest(maxBatch, Long.MAX_VALUE,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        worker.submit(request);

        // Wait a bit for the fetch loop to pick up the request and add messages.
        Thread.sleep(500);

        // Shutdown before batch completes.
        List<MessageView> remaining = worker.shutdown();
        worker = null;

        // The request should have been cancelled, and messages should be returned.
        assertTrue(request.future.isDone());
        // The 3 messages should be in 'remaining'.
        assertEquals(3, remaining.size());
    }

    // -----------------------------------------------------------------------
    // 7. Shutdown with no pending requests returns empty list
    // -----------------------------------------------------------------------

    @Test
    public void testShutdownNoPendingReturnsEmpty() {
        BatchPolicy policy = new BatchPolicy(10, Duration.ofSeconds(5));
        worker = createAndStartWorker();

        List<MessageView> remaining = worker.shutdown();
        worker = null;

        assertTrue(remaining.isEmpty());
    }

    // -----------------------------------------------------------------------
    // 8. Delegate exception triggers retry, not request failure
    // -----------------------------------------------------------------------

    @Test
    public void testDelegateExceptionRetries() throws Exception {
        final int maxBatch = 2;
        BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        worker = createAndStartWorker();

        // First call throws, second call succeeds.
        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenThrow(new ClientException("transient error"))
            .thenReturn(fakeMessages(maxBatch, 10));

        BatchRequest request = new BatchRequest(maxBatch, Long.MAX_VALUE,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        worker.submit(request);

        // Should eventually succeed after retry.
        List<MessageView> result = request.future.get(10, TimeUnit.SECONDS);
        assertEquals(maxBatch, result.size());
    }

    // -----------------------------------------------------------------------
    // 9. Multiple fetches accumulate into one batch
    // -----------------------------------------------------------------------

    @Test
    public void testMultipleFetchesAccumulate() throws Exception {
        final int maxBatch = 5;
        BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        worker = createAndStartWorker();

        // Each call returns 2 messages; need 3 calls to reach 5 (2+2+2, but 5 triggers at 5th msg).
        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenReturn(fakeMessages(2, 10))
            .thenReturn(fakeMessages(2, 10))
            .thenReturn(fakeMessages(2, 10));

        BatchRequest request = new BatchRequest(maxBatch, Long.MAX_VALUE,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        worker.submit(request);

        List<MessageView> result = request.future.get(5, TimeUnit.SECONDS);
        // 2+2+2=6, isFull triggers at messages.size() >= 5, so 5th message triggers completion.
        assertEquals(maxBatch, result.size());
    }

    // -----------------------------------------------------------------------
    // 10. Demand-driven: no receive call when no request submitted
    // -----------------------------------------------------------------------

    @Test
    public void testDemandDrivenNoReceiveWithoutRequest() throws Exception {
        BatchPolicy policy = new BatchPolicy(10, Duration.ofSeconds(5));
        worker = createAndStartWorker();

        // Let the worker run idle for a bit.
        Thread.sleep(500);

        // Delegate.receive should never have been called.
        org.mockito.Mockito.verify(delegate, org.mockito.Mockito.never())
            .receive(anyInt(), any(Duration.class));
    }

    // -----------------------------------------------------------------------
    // 11. Concurrent submits from multiple threads
    // -----------------------------------------------------------------------

    @Test
    public void testConcurrentSubmits() throws Exception {
        final int maxBatch = 1;
        final int threadCount = 10;
        BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        worker = createAndStartWorker();

        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenAnswer(invocation -> fakeMessages(1, 10));

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        List<CompletableFuture<List<MessageView>>> futures =
            Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                BatchRequest req = new BatchRequest(maxBatch, Long.MAX_VALUE,
                    policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
                worker.submit(req);
                futures.add(req.future);
                doneLatch.countDown();
            }).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS));

        // All futures should complete.
        for (CompletableFuture<List<MessageView>> f : futures) {
            List<MessageView> msgs = f.get(10, TimeUnit.SECONDS);
            assertEquals(1, msgs.size());
        }
    }

    // -----------------------------------------------------------------------
    // 12. isFull by both count and bytes boundary
    // -----------------------------------------------------------------------

    @Test
    public void testIsFullByCountExact() throws Exception {
        final int maxBatch = 3;
        BatchPolicy policy = new BatchPolicy(maxBatch, Duration.ofSeconds(5));
        worker = createAndStartWorker();

        // Return exactly maxBatch messages.
        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenReturn(fakeMessages(maxBatch, 10));

        BatchRequest request = new BatchRequest(maxBatch, Long.MAX_VALUE,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        worker.submit(request);

        List<MessageView> result = request.future.get(5, TimeUnit.SECONDS);
        assertEquals(maxBatch, result.size());
    }

    // -----------------------------------------------------------------------
    // 13. Overflow from bytes-limited batch is available for next request
    // -----------------------------------------------------------------------

    @Test
    public void testOverflowFromBytesLimitedBatch() throws Exception {
        final int bodySize = 512;
        // maxBatchBytes = 1KB, each body 512 bytes, so 2 messages fill the batch.
        final long maxBatchBytes = 1024L;
        final int maxBatchSize = 100;

        BatchPolicy policy = new BatchPolicy(maxBatchSize, maxBatchBytes, Duration.ofSeconds(5));
        worker = createAndStartWorker();

        // Return 4 messages; first batch takes 2, overflow gets 2.
        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenReturn(fakeMessages(4, bodySize));

        // First request
        BatchRequest req1 = new BatchRequest(maxBatchSize, maxBatchBytes,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        worker.submit(req1);
        List<MessageView> result1 = req1.future.get(5, TimeUnit.SECONDS);
        assertEquals(2, result1.size());

        // Second request uses overflow
        BatchRequest req2 = new BatchRequest(maxBatchSize, maxBatchBytes,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        worker.submit(req2);
        List<MessageView> result2 = req2.future.get(5, TimeUnit.SECONDS);
        assertEquals(2, result2.size());
    }

    // -----------------------------------------------------------------------
    // 14. Shutdown cancels pending unfulfilled requests
    // -----------------------------------------------------------------------

    @Test
    public void testShutdownCancelsPendingRequests() throws Exception {
        BatchPolicy policy = new BatchPolicy(100, Duration.ofSeconds(30));
        worker = createAndStartWorker();

        // Delegate blocks forever, so requests never complete.
        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenAnswer(invocation -> {
                Thread.sleep(60_000);
                return Collections.emptyList();
            });

        BatchRequest req1 = new BatchRequest(100, Long.MAX_VALUE,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        BatchRequest req2 = new BatchRequest(100, Long.MAX_VALUE,
            policy.getMaxWaitTime().toNanos(), INVISIBLE_DURATION);
        worker.submit(req1);
        worker.submit(req2);

        // Give worker time to start processing req1.
        Thread.sleep(300);

        worker.shutdown();
        worker = null;

        // Both should be done (cancelled).
        assertTrue(req1.future.isDone());
        assertTrue(req2.future.isDone());
    }

    // -----------------------------------------------------------------------
    // 15. Cache eviction: idle overflow messages are released
    // -----------------------------------------------------------------------

    @Test
    public void testCacheEvictionReleasesIdleMessages() throws Exception {
        // Use a very short eviction time for testing.
        final Duration evictionTime = Duration.ofMillis(200);
        worker = createAndStartWorker(evictionTime);

        final int maxBatch = 2;
        // Delegate returns 5 messages; first request takes 2, 3 go to overflow.
        when(delegate.receive(anyInt(), any(Duration.class)))
            .thenReturn(fakeMessages(5, 10))
            .thenAnswer(invocation -> {
                Thread.sleep(60_000);
                return Collections.emptyList();
            });

        // Mock changeInvisibleDurationAsync to return completed future.
        when(delegate.changeInvisibleDurationAsync(any(MessageView.class), any(Duration.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        BatchRequest req = new BatchRequest(maxBatch, Long.MAX_VALUE,
            Duration.ofSeconds(5).toNanos(), INVISIBLE_DURATION);
        worker.submit(req);
        req.future.get(5, TimeUnit.SECONDS);

        // Wait for eviction to fire (eviction check interval = max(200/3, 1000) = 1000ms).
        // But with 200ms eviction time and check interval max(66, 1000) = 1000ms,
        // we need to wait a bit longer for the scheduled task to run.
        Thread.sleep(1500);

        // Verify that changeInvisibleDurationAsync was called for the evicted messages.
        verify(delegate, atLeastOnce())
            .changeInvisibleDurationAsync(any(MessageView.class), any(Duration.class));
    }
}
