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

import java.nio.ByteBuffer;
import java.util.Base64;

public class UUID {

    public static final java.util.UUID SENTINEL_ID = new java.util.UUID(0L, 1L);

    private java.util.UUID uuid;

    public UUID(long mostSigBits, long leastSigBits) {
        this.uuid = new java.util.UUID(mostSigBits, leastSigBits);
    }

    private UUID(java.util.UUID uuid) {
        this.uuid = uuid;
    }

    public UUID randomUUID() {
        java.util.UUID uuid = java.util.UUID.randomUUID();
        while (uuid.equals(SENTINEL_ID))    {
            uuid = java.util.UUID.randomUUID();
        }
        return new UUID(uuid);
    }

    public java.util.UUID toJavaUUID() {
        return uuid;
    }

   public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || !(obj instanceof UUID)) return false;
        java.util.UUID uuid = ((UUID) obj).toJavaUUID();
        return this.toJavaUUID().equals(uuid);
   }

   public String toString() {
       Base64.getUrlEncoder().withoutPadding().encodeToString(getBytesFromUuid(uuid));
   }

   private byte[] getBytesFromUuid(java.util.UUID uuid){
        // Extract bytes for uuid which is 128 bits (or 16 bytes) long.
        ByteBuffer uuidBytes = ByteBuffer.wrap(new byte[16]);
        uuidBytes.putLong(uuid.getMostSignificantBits());
        uuidBytes.putLong(uuid.getLeastSignificantBits());
        return uuidBytes.array();
    }
}

