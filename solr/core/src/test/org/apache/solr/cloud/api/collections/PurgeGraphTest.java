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

package org.apache.solr.cloud.api.collections;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ObjectArrays;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.api.collections.DeleteBackupCmd.PurgeGraph;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.BackupFilePaths;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PurgeGraphTest extends SolrTestCaseJ4 {
    private static final String[] shardBackupIds = new String[]{"b1_s1", "b1_s2", "b2_s1", "b2_s2", "b3_s1", "b3_s2", "b3_s3"};

    @Test
    public void test() throws URISyntaxException, IOException {
        assumeWorkingMockito();
        BackupRepository repository = mock(BackupRepository.class);
        BackupFilePaths paths = mock(BackupFilePaths.class);
        when(paths.getBackupLocation()).thenReturn(new URI("/temp"));
        when(paths.getIndexDir()).thenReturn(new URI("/temp/index"));
        when(paths.getShardBackupMetadataDir()).thenReturn(new URI("/temp/backup_point"));

        PurgeGraph purgeGraph = new PurgeGraph();
        buildCompleteGraph(repository, paths, purgeGraph);
        purgeGraph.findDeletableNodes(repository, paths);

        assertEquals(0, purgeGraph.backupIdDeletes.size());
        assertEquals(0, purgeGraph.shardBackupMetadataDeletes.size());
        assertEquals(0, purgeGraph.indexFileDeletes.size());

        testDeleteUnreferencedFiles(repository, paths, purgeGraph);
        testMissingBackupPointFiles(repository, paths);
        testMissingIndexFiles(repository, paths);
    }

    private void testMissingIndexFiles(BackupRepository repository, BackupFilePaths paths) throws IOException {
        PurgeGraph purgeGraph = new PurgeGraph();
        buildCompleteGraph(repository, paths, purgeGraph);

        Set<String> indexFiles = purgeGraph.indexFileNodeMap.keySet();
        when(repository.listAllOrEmpty(same(paths.getIndexDir()))).thenAnswer((Answer<String[]>) invocationOnMock -> {
            Set<String> newFiles = new HashSet<>(indexFiles);
            newFiles.remove("s1_102");
            return newFiles.toArray(new String[0]);
        });
        purgeGraph.findDeletableNodes(repository, paths);

        assertEquals(3, purgeGraph.backupIdDeletes.size());
        assertEquals(shardBackupIds.length, purgeGraph.shardBackupMetadataDeletes.size());
        assertEquals(purgeGraph.indexFileNodeMap.size(), purgeGraph.indexFileDeletes.size() + 1);

        purgeGraph = new PurgeGraph();
        buildCompleteGraph(repository, paths, purgeGraph);

        Set<String> indexFiles2 = purgeGraph.indexFileNodeMap.keySet();
        when(repository.listAllOrEmpty(same(paths.getIndexDir()))).thenAnswer((Answer<String[]>) invocationOnMock -> {
            Set<String> newFiles = new HashSet<>(indexFiles2);
            newFiles.remove("s1_101");
            return newFiles.toArray(new String[0]);
        });
        purgeGraph.findDeletableNodes(repository, paths);

        assertEquals(2, purgeGraph.backupIdDeletes.size());
        assertEquals(4, purgeGraph.shardBackupMetadataDeletes.size());
        assertTrue(purgeGraph.indexFileDeletes.contains("s1_100"));
        assertFalse(purgeGraph.indexFileDeletes.contains("s1_101"));
    }

    private void testMissingBackupPointFiles(BackupRepository repository, BackupFilePaths paths) throws IOException {
        PurgeGraph purgeGraph = new PurgeGraph();
        buildCompleteGraph(repository, paths, purgeGraph);
        when(repository.listAllOrEmpty(same(paths.getShardBackupMetadataDir()))).thenAnswer((Answer<String[]>)
                invocationOnMock -> Arrays.copyOfRange(shardBackupIds, 1, shardBackupIds.length)
        );
        purgeGraph.findDeletableNodes(repository, paths);

        assertEquals(1, purgeGraph.backupIdDeletes.size());
        assertEquals("b1", purgeGraph.backupIdDeletes.get(0));
        assertEquals(1, purgeGraph.shardBackupMetadataDeletes.size());
        assertEquals("b1_s2", purgeGraph.shardBackupMetadataDeletes.get(0));
        assertTrue(purgeGraph.indexFileDeletes.contains("s1_100"));
        assertFalse(purgeGraph.indexFileDeletes.contains("s1_101"));

        purgeGraph = new PurgeGraph();
        buildCompleteGraph(repository, paths, purgeGraph);
        when(repository.listAllOrEmpty(same(paths.getShardBackupMetadataDir()))).thenAnswer((Answer<String[]>)
                invocationOnMock -> new String[]{"b1_s1", "b2_s1", "b3_s1", "b3_s2", "b3_s3"}
        );
        purgeGraph.findDeletableNodes(repository, paths);

        assertEquals(2, purgeGraph.backupIdDeletes.size());
        assertTrue(purgeGraph.backupIdDeletes.containsAll(Arrays.asList("b1", "b2")));
        assertEquals(2, purgeGraph.shardBackupMetadataDeletes.size());
        assertTrue(purgeGraph.shardBackupMetadataDeletes.containsAll(Arrays.asList("b2_s1", "b1_s1")));
        assertTrue(purgeGraph.indexFileDeletes.containsAll(Arrays.asList("s1_100", "s1_101")));
        assertFalse(purgeGraph.indexFileDeletes.contains("s1_102"));
    }

    private void testDeleteUnreferencedFiles(BackupRepository repository, BackupFilePaths paths,
                                             PurgeGraph purgeGraph) throws IOException {
        buildCompleteGraph(repository, paths, purgeGraph);
        String[] unRefBackupPoints = addUnRefFiles(repository, "b4_s", paths.getShardBackupMetadataDir());
        String[] unRefIndexFiles = addUnRefFiles(repository, "s4_", paths.getIndexDir());

        purgeGraph.findDeletableNodes(repository, paths);

        assertEquals(0, purgeGraph.backupIdDeletes.size());
        assertEquals(unRefBackupPoints.length, purgeGraph.shardBackupMetadataDeletes.size());
        assertTrue(purgeGraph.shardBackupMetadataDeletes.containsAll(Arrays.asList(unRefBackupPoints)));
        assertEquals(unRefIndexFiles.length, purgeGraph.indexFileDeletes.size());
        assertTrue(purgeGraph.indexFileDeletes.containsAll(Arrays.asList(unRefIndexFiles)));
    }

    private String[] addUnRefFiles(BackupRepository repository, String prefix, URI dir) {
        String[] unRefBackupPoints = new String[random().nextInt(10) + 1];
        for (int i = 0; i < unRefBackupPoints.length; i++) {
            unRefBackupPoints[i] = prefix + (100 + i);
        }
        String[] shardBackupMetadataFiles = repository.listAllOrEmpty(dir);
        when(repository.listAllOrEmpty(same(dir)))
                .thenAnswer((Answer<String[]>) invocation
                        -> ObjectArrays.concat(shardBackupMetadataFiles, unRefBackupPoints, String.class));
        return unRefBackupPoints;
    }

    private void buildCompleteGraph(BackupRepository repository, BackupFilePaths paths,
                                    PurgeGraph purgeGraph) throws IOException {
        when(repository.listAllOrEmpty(same(paths.getShardBackupMetadataDir()))).thenAnswer((Answer<String[]>) invocationOnMock -> shardBackupIds);
        //logical

        for (String shardBackupId : shardBackupIds) {
            purgeGraph.addEdge(purgeGraph.getShardBackupIdNode(shardBackupId),
                    purgeGraph.getBackupIdNode(shardBackupId.substring(0, 2)));
            for (int i = 0; i < random().nextInt(30); i++) {
                String fileName = shardBackupId.substring(3) + "_" + random().nextInt(15);
                purgeGraph.addEdge(purgeGraph.getShardBackupIdNode(shardBackupId),
                        purgeGraph.getIndexFileNode(fileName));
            }
        }

        purgeGraph.addEdge(purgeGraph.getShardBackupIdNode("b1_s1"),
                purgeGraph.getIndexFileNode("s1_100"));

        purgeGraph.addEdge(purgeGraph.getShardBackupIdNode("b1_s1"),
                purgeGraph.getIndexFileNode("s1_101"));
        purgeGraph.addEdge(purgeGraph.getShardBackupIdNode("b2_s1"),
                purgeGraph.getIndexFileNode("s1_101"));

        purgeGraph.addEdge(purgeGraph.getShardBackupIdNode("b1_s1"),
                purgeGraph.getIndexFileNode("s1_102"));
        purgeGraph.addEdge(purgeGraph.getShardBackupIdNode("b2_s1"),
                purgeGraph.getIndexFileNode("s1_102"));
        purgeGraph.addEdge(purgeGraph.getShardBackupIdNode("b3_s1"),
                purgeGraph.getIndexFileNode("s1_102"));

        when(repository.listAllOrEmpty(same(paths.getIndexDir()))).thenAnswer((Answer<String[]>) invocationOnMock ->
                purgeGraph.indexFileNodeMap.keySet().toArray(new String[0]));
    }
}
