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
package org.apache.kafka.server.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FeatureVersionTest {

    @ParameterizedTest
    @EnumSource(FeatureVersion.class)
    public void testFromFeatureLevelAllFeatures(FeatureVersion feature) {
        FeatureVersionUtils.FeatureVersionImpl[] featureImplementations = feature.features();
        int numFeatures = featureImplementations.length;
        for (short i = 0; i < numFeatures; i++) {
            assertEquals(featureImplementations[i], feature.fromFeatureLevel(i));
        }
    }

    @ParameterizedTest
    @EnumSource(FeatureVersion.class)
    public void testValidateVersionAllFeatures(FeatureVersion feature) {
        for (FeatureVersionUtils.FeatureVersionImpl featureImpl : feature.features()) {
            // Ensure that the feature is valid given the typical metadataVersionMapping and the dependencies.
            // Note: Other metadata versions are valid, but this one should always be valid.
            FeatureVersion.validateVersion(featureImpl, featureImpl.metadataVersionMapping(), featureImpl.dependencies());
        }
    }

    @Test
    public void testInvalidValidateVersion() {
        // Using too low of a MetadataVersion is invalid
        assertThrows(IllegalArgumentException.class,
            () -> FeatureVersion.validateVersion(
                TestFeatureVersion.TEST_1,
                MetadataVersion.IBP_2_8_IV0,
                Collections.emptyMap()
            )
        );

        // Using a version that is lower than the dependency will fail.
        assertThrows(IllegalArgumentException.class,
             () -> FeatureVersion.validateVersion(
                 TestFeatureVersion.TEST_2,
                 MetadataVersion.MINIMUM_BOOTSTRAP_VERSION,
                 Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_7_IV0.featureLevel())
             )
        );
    }

    @ParameterizedTest
    @EnumSource(FeatureVersion.class)
    public void testDefaultValueAllFeatures(FeatureVersion feature) {
        for (FeatureVersionUtils.FeatureVersionImpl featureImpl : feature.features()) {
            assertEquals(feature.defaultValue(Optional.of(featureImpl.metadataVersionMapping())), featureImpl.featureLevel(),
                    "Failed to get the correct default for " + featureImpl);
        }
    }

    @ParameterizedTest
    @EnumSource(MetadataVersion.class)
    public void testDefaultTestVersion(MetadataVersion metadataVersion) {
        short expectedVersion;
        if (!metadataVersion.isLessThan(MetadataVersion.IBP_3_8_IV0)) {
            expectedVersion = 2;
        } else if (!metadataVersion.isLessThan(MetadataVersion.IBP_3_7_IV0)) {
            expectedVersion = 1;
        } else {
            expectedVersion = 0;
        }
        assertEquals(expectedVersion, FeatureVersion.TEST_VERSION.defaultValue(Optional.of(metadataVersion)));
    }

    @Test
    public void testEmptyDefaultUsesLatestProduction() {
        assertEquals(FeatureVersion.TEST_VERSION.defaultValue(Optional.empty()),
                FeatureVersion.TEST_VERSION.defaultValue(Optional.of(MetadataVersion.LATEST_PRODUCTION)));
    }
}
