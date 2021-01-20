/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.kafka.common.requests.FetchMetadata.INITIAL_EPOCH;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A unit test for FetchSessionHandler.
 */
@Timeout(120)
public class FetchSessionHandlerTest {
    private static final LogContext LOG_CONTEXT = new LogContext("[FetchSessionHandler]=");
    private final static Map<String, Uuid> EMPTY_IDS = new HashMap<>();

    private static Map<Uuid, String> topicNames = new HashMap<>();
    private static Map<String, Uuid> topicIds = new HashMap<>();

    /**
     * Create a set of TopicPartitions.  We use a TreeSet, in order to get a deterministic
     * ordering for test purposes.
     */
    private static Set<TopicPartition> toSet(TopicPartition... arr) {
        TreeSet<TopicPartition> set = new TreeSet<>(new Comparator<TopicPartition>() {
            @Override
            public int compare(TopicPartition o1, TopicPartition o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });
        set.addAll(Arrays.asList(arr));
        return set;
    }

    @Test
    public void testFindMissing() {
        TopicPartition foo0 = new TopicPartition("foo", 0);
        TopicPartition foo1 = new TopicPartition("foo", 1);
        TopicPartition bar0 = new TopicPartition("bar", 0);
        TopicPartition bar1 = new TopicPartition("bar", 1);
        TopicPartition baz0 = new TopicPartition("baz", 0);
        TopicPartition baz1 = new TopicPartition("baz", 1);
        assertEquals(toSet(), FetchSessionHandler.findMissing(toSet(foo0), toSet(foo0)));
        assertEquals(toSet(foo0), FetchSessionHandler.findMissing(toSet(foo0), toSet(foo1)));
        assertEquals(toSet(foo0, foo1),
            FetchSessionHandler.findMissing(toSet(foo0, foo1), toSet(baz0)));
        assertEquals(toSet(bar1, foo0, foo1),
            FetchSessionHandler.findMissing(toSet(foo0, foo1, bar0, bar1),
                toSet(bar0, baz0, baz1)));
        assertEquals(toSet(),
            FetchSessionHandler.findMissing(toSet(foo0, foo1, bar0, bar1, baz1),
                toSet(foo0, foo1, bar0, bar1, baz0, baz1)));
    }

    private static final class ReqEntry {
        final TopicPartition part;
        final FetchRequest.PartitionData data;

        ReqEntry(String topic, int partition, long fetchOffset, long logStartOffset, int maxBytes) {
            this.part = new TopicPartition(topic, partition);
            this.data = new FetchRequest.PartitionData(fetchOffset, logStartOffset, maxBytes, Optional.empty());
        }
    }

    private static LinkedHashMap<TopicPartition, FetchRequest.PartitionData> reqMap(ReqEntry... entries) {
        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> map = new LinkedHashMap<>();
        for (ReqEntry entry : entries) {
            map.put(entry.part, entry.data);
        }
        return map;
    }

    private static void assertMapEquals(Map<TopicPartition, FetchRequest.PartitionData> expected,
                                        Map<TopicPartition, FetchRequest.PartitionData> actual) {
        Iterator<Map.Entry<TopicPartition, FetchRequest.PartitionData>> expectedIter =
            expected.entrySet().iterator();
        Iterator<Map.Entry<TopicPartition, FetchRequest.PartitionData>> actualIter =
            actual.entrySet().iterator();
        int i = 1;
        while (expectedIter.hasNext()) {
            Map.Entry<TopicPartition, FetchRequest.PartitionData> expectedEntry = expectedIter.next();
            if (!actualIter.hasNext()) {
                fail("Element " + i + " not found.");
            }
            Map.Entry<TopicPartition, FetchRequest.PartitionData> actuaLEntry = actualIter.next();
            assertEquals(expectedEntry.getKey(), actuaLEntry.getKey(), "Element " + i +
                " had a different TopicPartition than expected.");
            assertEquals(expectedEntry.getValue(), actuaLEntry.getValue(), "Element " + i +
                " had different PartitionData than expected.");
            i++;
        }
        if (expectedIter.hasNext()) {
            fail("Unexpected element " + i + " found.");
        }
    }

    @SafeVarargs
    private static void assertMapsEqual(Map<TopicPartition, FetchRequest.PartitionData> expected,
                                        Map<TopicPartition, FetchRequest.PartitionData>... actuals) {
        for (Map<TopicPartition, FetchRequest.PartitionData> actual : actuals) {
            assertMapEquals(expected, actual);
        }
    }

    private static void assertListEquals(List<TopicPartition> expected, List<TopicPartition> actual) {
        for (TopicPartition expectedPart : expected) {
            if (!actual.contains(expectedPart)) {
                fail("Failed to find expected partition " + expectedPart);
            }
        }
        for (TopicPartition actualPart : actual) {
            if (!expected.contains(actualPart)) {
                fail("Found unexpected partition " + actualPart);
            }
        }
    }

    private static final class RespEntry {
        final TopicPartition part;
        final FetchResponse.PartitionData<MemoryRecords> data;

        RespEntry(String topic, int partition, long highWatermark, long lastStableOffset) {
            this.part = new TopicPartition(topic, partition);
            this.data = new FetchResponse.PartitionData<>(
                Errors.NONE,
                highWatermark,
                lastStableOffset,
                0,
                null,
                null);
        }
    }

    private static LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> respMap(RespEntry... entries) {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> map = new LinkedHashMap<>();
        for (RespEntry entry : entries) {
            map.put(entry.part, entry.data);
        }
        return map;
    }

    /**
     * Test the handling of SESSIONLESS responses.
     * Pre-KIP-227 brokers always supply this kind of response.
     */
    @Test
    public void testSessionless() {
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        addTopicId("foo");
        builder.add(new TopicPartition("foo", 0), topicIds.get("foo"),
            new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder.add(new TopicPartition("foo", 1), topicIds.get("foo"),
            new FetchRequest.PartitionData(10, 110, 210, Optional.empty()));
        FetchSessionHandler.FetchRequestData data = builder.build();
        assertMapsEqual(reqMap(new ReqEntry("foo", 0, 0, 100, 200),
                               new ReqEntry("foo", 1, 10, 110, 210)),
            data.toSend(), data.sessionPartitions());
        assertEquals(INVALID_SESSION_ID, data.metadata().sessionId());
        assertEquals(INITIAL_EPOCH, data.metadata().epoch());

        // Double wrap the response so we get the behavior where responseMap is null
        FetchResponse<MemoryRecords> resp = FetchResponse.prepareResponse(Errors.NONE,
            respMap(new RespEntry("foo", 0, 0, 0),
                    new RespEntry("foo", 1, 0, 0)),
                Collections.emptyList(), topicIds,
            0, INVALID_SESSION_ID);
        handler.handleResponse(resp, ApiKeys.FETCH.latestVersion());

        FetchSessionHandler.Builder builder2 = handler.newBuilder();
        builder2.add(new TopicPartition("foo", 0), topicIds.get("foo"),
            new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data2 = builder2.build();
        assertEquals(INVALID_SESSION_ID, data2.metadata().sessionId());
        assertEquals(INITIAL_EPOCH, data2.metadata().epoch());
        assertMapsEqual(reqMap(new ReqEntry("foo", 0, 0, 100, 200)),
            data.toSend(), data.sessionPartitions());
    }

    @Test
    public void testSessionlessOldVersion() {
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(new TopicPartition("foo", 0), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder.add(new TopicPartition("foo", 1), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(10, 110, 210, Optional.empty()));
        FetchSessionHandler.FetchRequestData data = builder.build();
        assertMapsEqual(reqMap(new ReqEntry("foo", 0, 0, 100, 200),
                new ReqEntry("foo", 1, 10, 110, 210)),
                data.toSend(), data.sessionPartitions());
        assertEquals(INVALID_SESSION_ID, data.metadata().sessionId());
        assertEquals(INITIAL_EPOCH, data.metadata().epoch());

        FetchResponse<MemoryRecords> resp = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 0, 0, 0),
                        new RespEntry("foo", 1, 0, 0)),
                Collections.emptyList(), EMPTY_IDS,
                0, INVALID_SESSION_ID);
        handler.handleResponse(resp, (short) 12);

        FetchSessionHandler.Builder builder2 = handler.newBuilder();
        builder2.add(new TopicPartition("foo", 0), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data2 = builder2.build();
        assertEquals(INVALID_SESSION_ID, data2.metadata().sessionId());
        assertEquals(INITIAL_EPOCH, data2.metadata().epoch());
        assertMapsEqual(reqMap(new ReqEntry("foo", 0, 0, 100, 200)),
                data.toSend(), data.sessionPartitions());
    }

    /**
     * Test handling an incremental fetch session.
     */
    @Test
    public void testIncrementals() {
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        addTopicId("foo");
        builder.add(new TopicPartition("foo", 0), topicIds.get("foo"),
            new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder.add(new TopicPartition("foo", 1), topicIds.get("foo"),
            new FetchRequest.PartitionData(10, 110, 210, Optional.empty()));
        FetchSessionHandler.FetchRequestData data = builder.build();
        assertMapsEqual(reqMap(new ReqEntry("foo", 0, 0, 100, 200),
            new ReqEntry("foo", 1, 10, 110, 210)),
            data.toSend(), data.sessionPartitions());
        assertEquals(INVALID_SESSION_ID, data.metadata().sessionId());
        assertEquals(INITIAL_EPOCH, data.metadata().epoch());

        // Double wrap the response so we get the behavior where responseMap is null
        FetchResponse<MemoryRecords> resp = FetchResponse.prepareResponse(Errors.NONE,
            respMap(new RespEntry("foo", 0, 10, 20),
                    new RespEntry("foo", 1, 10, 20)),
            Collections.emptyList(), topicIds, 0, 123);
        handler.handleResponse(resp, ApiKeys.FETCH.latestVersion());

        // Test an incremental fetch request which adds one partition and modifies another.
        FetchSessionHandler.Builder builder2 = handler.newBuilder();
        addTopicId("bar");
        builder2.add(new TopicPartition("foo", 0), topicIds.get("foo"),
            new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder2.add(new TopicPartition("foo", 1), topicIds.get("foo"),
            new FetchRequest.PartitionData(10, 120, 210, Optional.empty()));
        builder2.add(new TopicPartition("bar", 0), topicIds.get("bar"),
            new FetchRequest.PartitionData(20, 200, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data2 = builder2.build();
        assertFalse(data2.metadata().isFull());
        assertMapEquals(reqMap(new ReqEntry("foo", 0, 0, 100, 200),
                new ReqEntry("foo", 1, 10, 120, 210),
                new ReqEntry("bar", 0, 20, 200, 200)),
            data2.sessionPartitions());
        assertMapEquals(reqMap(new ReqEntry("bar", 0, 20, 200, 200),
                new ReqEntry("foo", 1, 10, 120, 210)),
            data2.toSend());

        FetchResponse<MemoryRecords> resp2 = FetchResponse.prepareResponse(Errors.NONE,
            respMap(new RespEntry("foo", 1, 20, 20)),
                Collections.emptyList(), topicIds, 0, 123);
        handler.handleResponse(resp2, ApiKeys.FETCH.latestVersion());

        // Skip building a new request.  Test that handling an invalid fetch session epoch response results
        // in a request which closes the session.
        FetchResponse<MemoryRecords> resp3 = FetchResponse.prepareResponse(Errors.INVALID_FETCH_SESSION_EPOCH, respMap(),
            Collections.emptyList(), topicIds, 0, INVALID_SESSION_ID);
        handler.handleResponse(resp3, ApiKeys.FETCH.latestVersion());

        FetchSessionHandler.Builder builder4 = handler.newBuilder();
        builder4.add(new TopicPartition("foo", 0), topicIds.get("foo"),
            new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder4.add(new TopicPartition("foo", 1), topicIds.get("foo"),
            new FetchRequest.PartitionData(10, 120, 210, Optional.empty()));
        builder4.add(new TopicPartition("bar", 0), topicIds.get("bar"),
            new FetchRequest.PartitionData(20, 200, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data4 = builder4.build();
        assertTrue(data4.metadata().isFull());
        assertEquals(data2.metadata().sessionId(), data4.metadata().sessionId());
        assertEquals(INITIAL_EPOCH, data4.metadata().epoch());
        assertMapsEqual(reqMap(new ReqEntry("foo", 0, 0, 100, 200),
            new ReqEntry("foo", 1, 10, 120, 210),
            new ReqEntry("bar", 0, 20, 200, 200)),
            data4.sessionPartitions(), data4.toSend());
    }

    @Test
    public void testIncrementalsOldVersion() {
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(new TopicPartition("foo", 0), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder.add(new TopicPartition("foo", 1), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(10, 110, 210, Optional.empty()));
        FetchSessionHandler.FetchRequestData data = builder.build();
        assertMapsEqual(reqMap(new ReqEntry("foo", 0, 0, 100, 200),
                new ReqEntry("foo", 1, 10, 110, 210)),
                data.toSend(), data.sessionPartitions());
        assertEquals(INVALID_SESSION_ID, data.metadata().sessionId());
        assertEquals(INITIAL_EPOCH, data.metadata().epoch());

        FetchResponse<MemoryRecords> resp = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 0, 10, 20),
                        new RespEntry("foo", 1, 10, 20)),
                Collections.emptyList(), EMPTY_IDS, 0, 123);
        handler.handleResponse(resp, (short) 12);

        // Test an incremental fetch request which adds one partition and modifies another.
        FetchSessionHandler.Builder builder2 = handler.newBuilder();
        builder2.add(new TopicPartition("foo", 0),  Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder2.add(new TopicPartition("foo", 1),  Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(10, 120, 210, Optional.empty()));
        builder2.add(new TopicPartition("bar", 0),  Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(20, 200, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data2 = builder2.build();
        assertFalse(data2.metadata().isFull());
        assertMapEquals(reqMap(new ReqEntry("foo", 0, 0, 100, 200),
                new ReqEntry("foo", 1, 10, 120, 210),
                new ReqEntry("bar", 0, 20, 200, 200)),
                data2.sessionPartitions());
        assertMapEquals(reqMap(new ReqEntry("bar", 0, 20, 200, 200),
                new ReqEntry("foo", 1, 10, 120, 210)),
                data2.toSend());

        FetchResponse<MemoryRecords> resp2 = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 1, 20, 20)),
                Collections.emptyList(), EMPTY_IDS, 0, 123);
        handler.handleResponse(resp2, (short) 12);

        // Skip building a new request.  Test that handling an invalid fetch session epoch response results
        // in a request which closes the session.
        FetchResponse<MemoryRecords> resp3 = FetchResponse.prepareResponse(Errors.INVALID_FETCH_SESSION_EPOCH, respMap(),
                Collections.emptyList(), EMPTY_IDS, 0, INVALID_SESSION_ID);
        handler.handleResponse(resp3, (short) 12);

        FetchSessionHandler.Builder builder4 = handler.newBuilder();
        builder4.add(new TopicPartition("foo", 0), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder4.add(new TopicPartition("foo", 1), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(10, 120, 210, Optional.empty()));
        builder4.add(new TopicPartition("bar", 0), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(20, 200, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data4 = builder4.build();
        assertTrue(data4.metadata().isFull());
        assertEquals(data2.metadata().sessionId(), data4.metadata().sessionId());
        assertEquals(INITIAL_EPOCH, data4.metadata().epoch());
        assertMapsEqual(reqMap(new ReqEntry("foo", 0, 0, 100, 200),
                new ReqEntry("foo", 1, 10, 120, 210),
                new ReqEntry("bar", 0, 20, 200, 200)),
                data4.sessionPartitions(), data4.toSend());
    }

    /**
     * Test that calling FetchSessionHandler#Builder#build twice fails.
     */
    @Test
    public void testDoubleBuild() {
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(new TopicPartition("foo", 0), Uuid.randomUuid(),
            new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder.build();
        try {
            builder.build();
            fail("Expected calling build twice to fail.");
        } catch (Throwable t) {
            // expected
        }
    }

    @Test
    public void testIncrementalPartitionRemoval() {
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        addTopicId("foo");
        addTopicId("bar");
        builder.add(new TopicPartition("foo", 0), topicIds.get("foo"),
            new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder.add(new TopicPartition("foo", 1), topicIds.get("foo"),
            new FetchRequest.PartitionData(10, 110, 210, Optional.empty()));
        builder.add(new TopicPartition("bar", 0), topicIds.get("bar"),
            new FetchRequest.PartitionData(20, 120, 220, Optional.empty()));
        FetchSessionHandler.FetchRequestData data = builder.build();
        assertMapsEqual(reqMap(new ReqEntry("foo", 0, 0, 100, 200),
            new ReqEntry("foo", 1, 10, 110, 210),
            new ReqEntry("bar", 0, 20, 120, 220)),
            data.toSend(), data.sessionPartitions());
        assertTrue(data.metadata().isFull());

        // Build so we get null responseMap
        FetchResponse<MemoryRecords> resp = FetchResponse.prepareResponse(Errors.NONE,
            respMap(new RespEntry("foo", 0, 10, 20),
                    new RespEntry("foo", 1, 10, 20),
                    new RespEntry("bar", 0, 10, 20)),
                Collections.emptyList(), topicIds, 0, 123);
        handler.handleResponse(resp, ApiKeys.FETCH.latestVersion());

        // Test an incremental fetch request which removes two partitions.
        FetchSessionHandler.Builder builder2 = handler.newBuilder();
        builder2.add(new TopicPartition("foo", 1), topicIds.get("foo"),
            new FetchRequest.PartitionData(10, 110, 210, Optional.empty()));
        FetchSessionHandler.FetchRequestData data2 = builder2.build();
        assertFalse(data2.metadata().isFull());
        assertEquals(123, data2.metadata().sessionId());
        assertEquals(1, data2.metadata().epoch());
        assertMapEquals(reqMap(new ReqEntry("foo", 1, 10, 110, 210)),
            data2.sessionPartitions());
        assertMapEquals(reqMap(), data2.toSend());
        ArrayList<TopicPartition> expectedToForget2 = new ArrayList<>();
        expectedToForget2.add(new TopicPartition("foo", 0));
        expectedToForget2.add(new TopicPartition("bar", 0));
        assertListEquals(expectedToForget2, data2.toForget());

        // A FETCH_SESSION_ID_NOT_FOUND response triggers us to close the session.
        // The next request is a session establishing FULL request.
        FetchResponse<MemoryRecords> resp2 = FetchResponse.prepareResponse(Errors.FETCH_SESSION_ID_NOT_FOUND,
            respMap(), Collections.emptyList(), topicIds, 0, INVALID_SESSION_ID);
        handler.handleResponse(resp2, ApiKeys.FETCH.latestVersion());
        FetchSessionHandler.Builder builder3 = handler.newBuilder();
        builder3.add(new TopicPartition("foo", 0), topicIds.get("foo"),
            new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data3 = builder3.build();
        assertTrue(data3.metadata().isFull());
        assertEquals(INVALID_SESSION_ID, data3.metadata().sessionId());
        assertEquals(INITIAL_EPOCH, data3.metadata().epoch());
        assertMapsEqual(reqMap(new ReqEntry("foo", 0, 0, 100, 200)),
            data3.sessionPartitions(), data3.toSend());
    }

    @Test
    public void testIncrementalPartitionRemovalOldVersion() {
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(new TopicPartition("foo", 0), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder.add(new TopicPartition("foo", 1), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(10, 110, 210, Optional.empty()));
        builder.add(new TopicPartition("bar", 0), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(20, 120, 220, Optional.empty()));
        FetchSessionHandler.FetchRequestData data = builder.build();
        assertMapsEqual(reqMap(new ReqEntry("foo", 0, 0, 100, 200),
                new ReqEntry("foo", 1, 10, 110, 210),
                new ReqEntry("bar", 0, 20, 120, 220)),
                data.toSend(), data.sessionPartitions());
        assertTrue(data.metadata().isFull());

        FetchResponse<MemoryRecords> resp = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 0, 10, 20),
                        new RespEntry("foo", 1, 10, 20),
                        new RespEntry("bar", 0, 10, 20)),
                Collections.emptyList(), EMPTY_IDS, 0, 123);
        handler.handleResponse(resp, (short) 12);

        // Test an incremental fetch request which removes two partitions.
        FetchSessionHandler.Builder builder2 = handler.newBuilder();
        builder2.add(new TopicPartition("foo", 1), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(10, 110, 210, Optional.empty()));
        FetchSessionHandler.FetchRequestData data2 = builder2.build();
        assertFalse(data2.metadata().isFull());
        assertEquals(123, data2.metadata().sessionId());
        assertEquals(1, data2.metadata().epoch());
        assertMapEquals(reqMap(new ReqEntry("foo", 1, 10, 110, 210)),
                data2.sessionPartitions());
        assertMapEquals(reqMap(), data2.toSend());
        ArrayList<TopicPartition> expectedToForget2 = new ArrayList<>();
        expectedToForget2.add(new TopicPartition("foo", 0));
        expectedToForget2.add(new TopicPartition("bar", 0));
        assertListEquals(expectedToForget2, data2.toForget());

        // A FETCH_SESSION_ID_NOT_FOUND response triggers us to close the session.
        // The next request is a session establishing FULL request.
        FetchResponse<MemoryRecords> resp2 = FetchResponse.prepareResponse(Errors.FETCH_SESSION_ID_NOT_FOUND,
                respMap(), Collections.emptyList(), EMPTY_IDS, 0, INVALID_SESSION_ID);
        handler.handleResponse(resp2, (short) 12);
        FetchSessionHandler.Builder builder3 = handler.newBuilder();
        builder3.add(new TopicPartition("foo", 0), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        FetchSessionHandler.FetchRequestData data3 = builder3.build();
        assertTrue(data3.metadata().isFull());
        assertEquals(INVALID_SESSION_ID, data3.metadata().sessionId());
        assertEquals(INITIAL_EPOCH, data3.metadata().epoch());
        assertMapsEqual(reqMap(new ReqEntry("foo", 0, 0, 100, 200)),
                data3.sessionPartitions(), data3.toSend());
    }

    @Test
    public void testVerifyFullFetchResponsePartitions() throws Exception {
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        addTopicId("foo");
        addTopicId("bar");
        // Double wrap the response so we get the behavior where responseMap is null
        FetchResponse<MemoryRecords> resp1 = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 0, 10, 20),
                        new RespEntry("foo", 1, 10, 20),
                        new RespEntry("bar", 0, 10, 20)),
                Collections.emptyList(), topicIds, 0, INVALID_SESSION_ID);
        String issue = handler.verifyFullFetchResponsePartitions(resp1.responseData(topicNames).keySet(), resp1.topicIds(), ApiKeys.FETCH.latestVersion());
        assertTrue(issue.contains("extra"));
        assertFalse(issue.contains("omitted"));
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(new TopicPartition("foo", 0), topicIds.get("foo"),
            new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder.add(new TopicPartition("foo", 1), topicIds.get("foo"),
            new FetchRequest.PartitionData(10, 110, 210, Optional.empty()));
        builder.add(new TopicPartition("bar", 0), topicIds.get("bar"),
            new FetchRequest.PartitionData(20, 120, 220, Optional.empty()));
        builder.build();
        FetchResponse<MemoryRecords> resp2 = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 0, 10, 20),
                        new RespEntry("foo", 1, 10, 20),
                        new RespEntry("bar", 0, 10, 20)),
                Collections.emptyList(), topicIds, 0, INVALID_SESSION_ID);
        String issue2 = handler.verifyFullFetchResponsePartitions(resp2.responseData(topicNames).keySet(), resp2.topicIds(), ApiKeys.FETCH.latestVersion());
        assertTrue(issue2 == null);
        FetchResponse<MemoryRecords> resp3 = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 0, 10, 20),
                        new RespEntry("foo", 1, 10, 20)),
                Collections.emptyList(), topicIds, 0, INVALID_SESSION_ID);
        String issue3 = handler.verifyFullFetchResponsePartitions(resp3.responseData(topicNames).keySet(), resp3.topicIds(), ApiKeys.FETCH.latestVersion());
        assertFalse(issue3.contains("extra"));
        assertTrue(issue3.contains("omitted"));
    }

    @Test
    public void testVerifyFullFetchResponsePartitionsOldVersion() throws Exception {
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        FetchResponse<MemoryRecords> resp1 = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 0, 10, 20),
                        new RespEntry("foo", 1, 10, 20),
                        new RespEntry("bar", 0, 10, 20)),
                Collections.emptyList(), EMPTY_IDS, 0, INVALID_SESSION_ID);
        String issue = handler.verifyFullFetchResponsePartitions(resp1.responseData(topicNames).keySet(), resp1.topicIds(), (short) 12);
        assertTrue(issue.contains("extra"));
        assertFalse(issue.contains("omitted"));
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(new TopicPartition("foo", 0), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder.add(new TopicPartition("foo", 1), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(10, 110, 210, Optional.empty()));
        builder.add(new TopicPartition("bar", 0), Uuid.ZERO_UUID,
                new FetchRequest.PartitionData(20, 120, 220, Optional.empty()));
        builder.build();
        FetchResponse<MemoryRecords> resp2 = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 0, 10, 20),
                        new RespEntry("foo", 1, 10, 20),
                        new RespEntry("bar", 0, 10, 20)),
                Collections.emptyList(), EMPTY_IDS, 0, INVALID_SESSION_ID);
        String issue2 = handler.verifyFullFetchResponsePartitions(resp2.responseData(topicNames).keySet(), resp2.topicIds(), (short) 12);
        assertTrue(issue2 == null);
        FetchResponse<MemoryRecords> resp3 = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 0, 10, 20),
                        new RespEntry("foo", 1, 10, 20)),
                Collections.emptyList(), EMPTY_IDS, 0, INVALID_SESSION_ID);
        String issue3 = handler.verifyFullFetchResponsePartitions(resp3.responseData(topicNames).keySet(), resp3.topicIds(), (short) 12);
        assertFalse(issue3.contains("extra"));
        assertTrue(issue3.contains("omitted"));
    }

    @Test
    public void testVerifyFullFetchResponsePartitionsWithTopicIds() throws Exception {
        FetchSessionHandler handler = new FetchSessionHandler(LOG_CONTEXT, 1);
        addTopicId("foo");
        addTopicId("bar");
        Uuid extraId = Uuid.randomUuid();
        topicIds.put("extra", extraId);
        // Double wrap the response so we get the behavior where responseMap is null
        FetchResponse<MemoryRecords> resp1 = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 0, 10, 20),
                        new RespEntry("extra", 1, 10, 20),
                        new RespEntry("bar", 0, 10, 20)),
                Collections.emptyList(), topicIds, 0, INVALID_SESSION_ID);
        String issue = handler.verifyFullFetchResponsePartitions(resp1.responseData(topicNames).keySet(), resp1.topicIds(), ApiKeys.FETCH.latestVersion());
        assertTrue(issue.contains("extra="));
        assertTrue(issue.contains("extraIds="));
        assertFalse(issue.contains("omitted"));
        FetchSessionHandler.Builder builder = handler.newBuilder();
        builder.add(new TopicPartition("foo", 0), topicIds.get("foo"),
                new FetchRequest.PartitionData(0, 100, 200, Optional.empty()));
        builder.add(new TopicPartition("bar", 0), topicIds.get("bar"),
                new FetchRequest.PartitionData(20, 120, 220, Optional.empty()));
        builder.build();
        FetchResponse<MemoryRecords> resp2 = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 0, 10, 20),
                        new RespEntry("extra", 1, 10, 20),
                        new RespEntry("bar", 0, 10, 20)),
                Collections.emptyList(), topicIds, 0, INVALID_SESSION_ID);
        String issue2 = handler.verifyFullFetchResponsePartitions(resp2.responseData(topicNames).keySet(), resp2.topicIds(), ApiKeys.FETCH.latestVersion());
        assertFalse(issue2.contains("extra="));
        assertTrue(issue2.contains("extraIds="));
        assertFalse(issue2.contains("omitted"));
        topicNames.put(extraId, "extra");
        FetchResponse<MemoryRecords> resp3 = FetchResponse.prepareResponse(Errors.NONE,
                respMap(new RespEntry("foo", 0, 10, 20),
                        new RespEntry("bar", 0, 10, 20)),
                Collections.emptyList(), topicIds, 0, INVALID_SESSION_ID);
        String issue3 = handler.verifyFullFetchResponsePartitions(resp3.responseData(topicNames).keySet(), resp3.topicIds(), ApiKeys.FETCH.latestVersion());
        assertTrue(issue3 == null);
    }

    private void addTopicId(String name) {
        Uuid id = Uuid.randomUuid();
        topicIds.put(name, id);
        topicNames.put(id, name);
    }
}
