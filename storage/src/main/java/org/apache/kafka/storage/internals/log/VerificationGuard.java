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
package org.apache.kafka.storage.internals.log;

import java.util.concurrent.atomic.AtomicLong;

public class VerificationGuard {

    // The sentinel verification guard will be used as a default when no verification guard is provided.
    // It can not be used to verify a transaction is ongoing and its verificationGuardValue is always 0.
    public static final VerificationGuard SENTINEL_VERIFICATION_GUARD = new VerificationGuard(0);
    private static final AtomicLong INCREMENTING_ID = new AtomicLong(0L);
    private final long verificationGuardValue;

    public VerificationGuard() {
        verificationGuardValue = INCREMENTING_ID.incrementAndGet();
    }

    private VerificationGuard(long value) {
        verificationGuardValue = value;
    }

    @Override
    public String toString() {
        return "VerificationGuard: " + verificationGuardValue;
    }

    @Override
    public boolean equals(Object obj) {
        if ((null == obj) || (obj.getClass() != this.getClass()))
            return false;
        VerificationGuard guard = (VerificationGuard) obj;
        return verificationGuardValue == guard.verificationGuardValue();
    }

    @Override
    public int hashCode() {
        long value = verificationGuardValue;
        return (int) (value ^ (value >>> 32));
    }

    private long verificationGuardValue() {
        return verificationGuardValue;
    }

    public boolean verifiedBy(VerificationGuard verifyingGuard) {
        return verifyingGuard != SENTINEL_VERIFICATION_GUARD && verifyingGuard.equals(this);
    }
}
