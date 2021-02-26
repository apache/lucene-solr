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
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.Test;

public abstract class AbstractBackupRepositoryTest extends SolrTestCaseJ4 {

    protected abstract BackupRepository getRepository();

    protected abstract URI getBaseUri() throws URISyntaxException;

    @Test
    public void test() throws IOException, URISyntaxException {
        try (BackupRepository repo = getRepository()) {
            URI baseUri = repo.resolve(getBaseUri(), "tmp");
            if (repo.exists(baseUri)) {
                repo.deleteDirectory(baseUri);
            }
            assertFalse(repo.exists(baseUri));
            repo.createDirectory(baseUri);
            assertTrue(repo.exists(baseUri));
            assertEquals(0, repo.listAll(baseUri).length);

            // test nested structure
            URI tmpFolder = repo.resolve(baseUri, "tmpDir");
            repo.createDirectory(tmpFolder);
            assertEquals(repo.getPathType(tmpFolder), BackupRepository.PathType.DIRECTORY);
            addFile(repo, repo.resolve(tmpFolder, "file1"));
            addFile(repo, repo.resolve(tmpFolder, "file2"));
            assertEquals(repo.getPathType(repo.resolve(tmpFolder, "file1")), BackupRepository.PathType.FILE);
            String[] files = repo.listAll(tmpFolder);
            assertEquals(new HashSet<>(Arrays.asList("file1", "file2")), new HashSet<>(Arrays.asList(files)));

            URI tmpFolder2 = repo.resolve(tmpFolder, "tmpDir2");
            repo.createDirectory(tmpFolder2);
            addFile(repo, repo.resolve(tmpFolder2, "file3"));
            addFile(repo, repo.resolve(tmpFolder2, "file4"));
            addFile(repo, repo.resolve(tmpFolder2, "file5"));
            //2 files + 1 folder
            assertEquals(3, repo.listAll(tmpFolder).length);
            // create same directory must be a no-op
            repo.createDirectory(tmpFolder2);
            assertEquals(3, repo.listAll(tmpFolder2).length);
            assertTrue(repo.exists(tmpFolder2));
            assertTrue(repo.exists(repo.resolve(tmpFolder2, "file3")));
            try {
                repo.delete(tmpFolder2, Arrays.asList("file7", "file6"), false);
                fail("Delete non existence file leads to success");
            } catch (NoSuchFileException e) {
                // expected
            }
            repo.delete(tmpFolder2, Arrays.asList("file7", "file6"), true);
            repo.delete(tmpFolder2, Arrays.asList("file3", "file4"), true);
            assertEquals(1, repo.listAll(tmpFolder2).length);
            assertFalse(repo.exists(repo.resolve(tmpFolder2, "file3")));
            repo.deleteDirectory(tmpFolder);
            assertFalse(repo.exists(tmpFolder));
            assertFalse(repo.exists(repo.resolve(tmpFolder2, "file5")));
            assertFalse(repo.exists(repo.resolve(tmpFolder, "file1")));
        }
    }

    private void addFile(BackupRepository repo, URI file) throws IOException {
        try (OutputStream os = repo.createOutput(file)) {
            os.write(100);
            os.write(101);
            os.write(102);
        }
    }
}
