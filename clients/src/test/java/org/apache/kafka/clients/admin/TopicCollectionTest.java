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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;

public class TopicCollectionTest {

    @Test
    public void testTopicCollection() {

        List<Uuid> topicIds = Arrays.asList(Uuid.randomUuid(), Uuid.randomUuid(), Uuid.randomUuid());
        List<String> topicNames = Arrays.asList("foo", "bar");

        TopicCollection idCollection = TopicCollection.ofTopicIds(topicIds);
        assertEquals(TopicCollection.TopicAttribute.TOPIC_ID, idCollection.attribute());

        TopicCollection nameCollection = TopicCollection.ofTopicNames(topicNames);
        assertEquals(TopicCollection.TopicAttribute.TOPIC_NAME, nameCollection.attribute());

        assertEquals(topicIds, idCollection.topicIds());
        assertThrows(UnsupportedOperationException.class, () -> idCollection.topicNames());

        assertEquals(topicNames, nameCollection.topicNames());
        assertThrows(UnsupportedOperationException.class, () -> nameCollection.topicIds());
    }

}