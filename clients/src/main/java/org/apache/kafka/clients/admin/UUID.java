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
/*
 * This class defines an immutable universally unique identifier (UUID). It represents a 128-bit value.
 * More specifically, the random UUIDs generated by this class are variant 2 (Leach-Salz) version 4 UUIDs.
 * This is the same type of UUID as the ones generated by java.util.UUID.
 */
public class UUID {

    /**
     * A UUID reserved for internal topics. Will never be returned by the randomUUID method
     */
    public static final UUID SENTINEL_ID = new UUID(new java.util.UUID(0L, 1L));
    private static final java.util.UUID SENTINEL_ID_INTERNAL = new java.util.UUID(0L, 1L);

    /**
     * A UUID that represents a null or empty UUID. Will never be returned by the randomUUID method
     */
    public static final UUID EMPTY_UUID = new UUID(new java.util.UUID(0L,0L));
    private static final java.util.UUID EMPTY_ID_INTERNAL = new java.util.UUID(0L, 0L);

    private final java.util.UUID uuid;

    /**
     * Constructs a 128-bit type 4 UUID where the first long represents the the most significant 64 bits
     * and the second long represents the least significant 64 bits.
     */
    public UUID(long mostSigBits, long leastSigBits) {
        this.uuid = new java.util.UUID(mostSigBits, leastSigBits);
    }

    private UUID(java.util.UUID uuid) {
        this.uuid = uuid;
    }

    /**
     * Static factory to retrieve a type 4 (pseudo randomly generated) UUID.
     * Will never return the SENTINEL_ID reserved for internal topics or the empty UUID that represents nil.
     */
    public static UUID randomUUID() {
        java.util.UUID uuid = java.util.UUID.randomUUID();
        while (uuid.equals(SENTINEL_ID_INTERNAL) || uuid.equals(EMPTY_ID_INTERNAL)) {
            uuid = java.util.UUID.randomUUID();
        }
        return new UUID(uuid);
    }

    /**
     * Returns the most significant bits of the UUID's 128 value.
     */
    public long getMostSignificantBits() {
        return uuid.getMostSignificantBits();
    }

    /**
     * Returns the least significant bits of the UUID's 128 value.
     */
    public long getLeastSignificantBits() {
        return uuid.getLeastSignificantBits();
    }

    private java.util.UUID toJavaUUID() {
        return uuid;
    }

    /**
     * Returns true iff the obj is another UUID represented by the same two long values.
     */
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (!(obj instanceof UUID)) return false;
        java.util.UUID uuid = ((UUID) obj).toJavaUUID();
        return this.toJavaUUID().equals(uuid);
    }

    /**
     * Returns a base64 string encoding of the UUID.
     */
    public String toString() {
       return Base64.getUrlEncoder().withoutPadding().encodeToString(getBytesFromUuid(uuid));
   }

    /**
     * Creates a UUID based on a base64 string encoding used in the toString() method.
     */
    public static UUID fromString(String str) {
        ByteBuffer uuidBytes = ByteBuffer.wrap(Base64.getUrlDecoder().decode(str));
        return new UUID(uuidBytes.getLong(), uuidBytes.getLong());
    }


    private byte[] getBytesFromUuid(java.util.UUID uuid) {
        // Extract bytes for uuid which is 128 bits (or 16 bytes) long.
        ByteBuffer uuidBytes = ByteBuffer.wrap(new byte[16]);
        uuidBytes.putLong(uuid.getMostSignificantBits());
        uuidBytes.putLong(uuid.getLeastSignificantBits());
        return uuidBytes.array();
    }
}

