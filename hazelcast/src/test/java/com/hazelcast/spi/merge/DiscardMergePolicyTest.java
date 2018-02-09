/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.merge;

import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DiscardMergePolicyTest {

    private static final String EXISTING = "EXISTING";
    private static final String MERGING = "MERGING";

    protected SplitBrainMergePolicy policy;

    @Before
    public void setup() {
        policy = new DiscardMergePolicy();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_existingValueAbsent() {
        MergeDataHolder existing = null;
        MergeDataHolder merging = entryWithGivenValue(MERGING);

        assertNull(policy.merge(merging, existing));
    }

    @Test
    public void merge_existingValuePresent() {
        MergeDataHolder existing = entryWithGivenValue(EXISTING);
        MergeDataHolder merging = entryWithGivenValue(MERGING);

        assertEquals(EXISTING, policy.merge(merging, existing));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_mergingNull() {
        MergeDataHolder existing = entryWithGivenValue(EXISTING);
        MergeDataHolder merging = null;

        assertEquals(EXISTING, policy.merge(merging, existing));
    }

    @Test
    public void merge_bothValuesNull() {
        MergeDataHolder existing = entryWithGivenValue(null);
        MergeDataHolder merging = entryWithGivenValue(null);

        assertNull(policy.merge(merging, existing));
    }

    private MergeDataHolder entryWithGivenValue(String value) {
        MergeDataHolder entryView = mock(MergeDataHolder.class);
        try {
            when(entryView.getValue()).thenReturn(value);
            return entryView;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
