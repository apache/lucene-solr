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
package org.apache.solr.core.backup;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class ShardBackupIdTest extends SolrTestCaseJ4 {

    @Test
    public void testCanParseIDFromStringWithUnsplitShardName() {
        final String idString = "md_shard1_0";

        final ShardBackupId parsedId = ShardBackupId.from(idString);

        assertEquals("shard1", parsedId.getShardName());
        assertEquals(new BackupId(0), parsedId.getContainingBackupId());
    }

    @Test
    public void testCanParseIdFromStringWithSplitShardName() {
        final String idString = "md_shard2_0_5";

        final ShardBackupId parsedId = ShardBackupId.from(idString);

        assertEquals("shard2_0", parsedId.getShardName());
        assertEquals(new BackupId(5), parsedId.getContainingBackupId());
    }

    @Test
    public void testCanParseIdFromStringWithManySplitShardName() {
        final String idString = "md_shard2_0_1_3";
        final ShardBackupId parsedId = ShardBackupId.from(idString);

        assertEquals("shard2_0_1", parsedId.getShardName());
        assertEquals(new BackupId(3), parsedId.getContainingBackupId());
    }
}
