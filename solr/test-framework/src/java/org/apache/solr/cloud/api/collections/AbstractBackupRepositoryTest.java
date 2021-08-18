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

import com.google.common.collect.Lists;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public abstract class AbstractBackupRepositoryTest extends SolrTestCaseJ4 {

    private static final boolean IGNORE_NONEXISTENT = true;
    private static final boolean REPORT_NONEXISTENT = false;

    protected abstract BackupRepository getRepository();

    protected abstract URI getBaseUri() throws URISyntaxException;

    /**
     * Provide a base {@link BackupRepository} configuration for use by any tests that call {@link BackupRepository#init(NamedList)} explicitly.
     *
     * Useful for setting configuration properties required for specific BackupRepository implementations.
     */
    protected NamedList<Object> getBaseBackupRepositoryConfiguration() {
        return new NamedList<>();
    }

    @Test
    public void testCanReadProvidedConfigValues() throws Exception {
        final NamedList<Object> config = getBaseBackupRepositoryConfiguration();
        config.add("configKey1", "configVal1");
        config.add("configKey2", "configVal2");
        config.add("location", "foo");
        try (BackupRepository repo = getRepository()) {
            repo.init(config);
            assertEquals("configVal1", repo.getConfigProperty("configKey1"));
            assertEquals("configVal2", repo.getConfigProperty("configKey2"));
        }
    }

    @Test
    public void testCanChooseDefaultOrOverrideLocationValue() throws Exception {
        final NamedList<Object> config = getBaseBackupRepositoryConfiguration();
        config.add(CoreAdminParams.BACKUP_LOCATION, "someLocation");
        try (BackupRepository repo = getRepository()) {
            repo.init(config);
            assertEquals("someLocation", repo.getBackupLocation(null));
            assertEquals("someOverridingLocation", repo.getBackupLocation("someOverridingLocation"));
        }
    }

    @Test
    public void testCanDetermineWhetherFilesAndDirectoriesExist() throws Exception {
        try (BackupRepository repo = getRepository()) {
            // Create 'emptyDir/', 'nonEmptyDir/', and 'nonEmptyDir/file.txt'
            final URI emptyDirUri = repo.resolve(getBaseUri(), "emptyDir");
            final URI nonEmptyDirUri = repo.resolve(getBaseUri(), "nonEmptyDir");
            final URI nestedFileUri = repo.resolve(nonEmptyDirUri, "file.txt");
            repo.createDirectory(emptyDirUri);
            repo.createDirectory(nonEmptyDirUri);
            addFile(repo, nestedFileUri);

            assertTrue(repo.exists(emptyDirUri));
            assertTrue(repo.exists(nonEmptyDirUri));
            assertTrue(repo.exists(nestedFileUri));
            final URI nonexistedDirUri = repo.resolve(getBaseUri(), "nonexistentDir");
            assertFalse(repo.exists(nonexistedDirUri));
        }
    }

    @Test
    public void testCanDistinguishBetweenFilesAndDirectories() throws Exception {
        try (BackupRepository repo = getRepository()) {
            final URI emptyDirUri = repo.resolve(getBaseUri(), "emptyDir");
            final URI nonEmptyDirUri = repo.resolve(getBaseUri(), "nonEmptyDir");
            final URI nestedFileUri = repo.resolve(nonEmptyDirUri, "file.txt");
            repo.createDirectory(emptyDirUri);
            repo.createDirectory(nonEmptyDirUri);
            addFile(repo, nestedFileUri);

            assertEquals(BackupRepository.PathType.DIRECTORY, repo.getPathType(emptyDirUri));
            assertEquals(BackupRepository.PathType.DIRECTORY, repo.getPathType(nonEmptyDirUri));
            assertEquals(BackupRepository.PathType.FILE, repo.getPathType(nestedFileUri));
        }
    }

    @Test
    public void testArbitraryFileDataCanBeStoredAndRetrieved() throws Exception {
        // create a BR
        // store some binary data in a file
        // retrieve that binary data
        // validate that sent == retrieved
        try (BackupRepository repo = getRepository()) {
            final URI fileUri = repo.resolve(getBaseUri(), "file.txt");
            final byte[] storedBytes = new byte[] {'h', 'e', 'l', 'l', 'o'};
            try (final OutputStream os = repo.createOutput(fileUri)) {
                os.write(storedBytes);
            }

            final int expectedNumBytes = storedBytes.length;
            final byte[] retrievedBytes = new byte[expectedNumBytes];
            try (final IndexInput is = repo.openInput(getBaseUri(), "file.txt", new IOContext(IOContext.Context.READ))) {
                assertEquals(expectedNumBytes, is.length());
                is.readBytes(retrievedBytes, 0, expectedNumBytes);
            }
            assertArrayEquals(storedBytes, retrievedBytes);
        }
    }

    // TODO JEGERLOW, create separate test for creation of nested directory when parent doesn't exist
    @Test
    public void testCanDeleteEmptyOrFullDirectories() throws Exception {
        try (BackupRepository repo = getRepository()) {
            // Test deletion of empty and full directories
            final URI emptyDirUri = repo.resolve(getBaseUri(), "emptyDir");
            final URI nonEmptyDirUri = repo.resolve(getBaseUri(), "nonEmptyDir");
            final URI fileUri = repo.resolve(nonEmptyDirUri, "file.txt");
            repo.createDirectory(emptyDirUri);
            repo.createDirectory(nonEmptyDirUri);
            addFile(repo, fileUri);
            repo.deleteDirectory(emptyDirUri);
            repo.deleteDirectory(nonEmptyDirUri);
            assertFalse(repo.exists(emptyDirUri));
            assertFalse(repo.exists(nonEmptyDirUri));
            assertFalse(repo.exists(fileUri));

            // Delete the middle directory in a deeply nested structure (/nest1/nest2/nest3/nest4)
            final URI level1DeeplyNestedUri = repo.resolve(getBaseUri(), "nest1");
            final URI level2DeeplyNestedUri = repo.resolve(level1DeeplyNestedUri, "nest2");
            final URI level3DeeplyNestedUri = repo.resolve(level2DeeplyNestedUri, "nest3");
            final URI level4DeeplyNestedUri = repo.resolve(level3DeeplyNestedUri, "nest4");
            repo.createDirectory(level1DeeplyNestedUri);
            repo.createDirectory(level2DeeplyNestedUri);
            repo.createDirectory(level3DeeplyNestedUri);
            repo.createDirectory(level4DeeplyNestedUri);
            repo.deleteDirectory(level3DeeplyNestedUri);
            assertTrue(repo.exists(level1DeeplyNestedUri));
            assertTrue(repo.exists(level2DeeplyNestedUri));
            assertFalse(repo.exists(level3DeeplyNestedUri));
            assertFalse(repo.exists(level4DeeplyNestedUri));
        }
    }

    @Test
    public void testDirectoryCreationFailsIfParentDoesntExist() throws Exception {
        try (BackupRepository repo = getRepository()) {
            final URI nonExistentParentUri = repo.resolve(getBaseUri(), "nonExistentParent");
            final URI nestedUri = repo.resolve(nonExistentParentUri, "childDirectoryToCreate");

            repo.createDirectory(nestedUri);
        }
    }

    @Test
    public void testCanDeleteIndividualFiles() throws Exception {
        try (BackupRepository repo = getRepository()) {
            final URI file1Uri = repo.resolve(getBaseUri(), "file1.txt");
            final URI file2Uri = repo.resolve(getBaseUri(), "file2.txt");
            final URI file3Uri = repo.resolve(getBaseUri(), "file3.txt");
            addFile(repo, file1Uri);
            addFile(repo, file2Uri);
            addFile(repo, file3Uri);

            // Ensure nonexistent files are handled differently based on boolean flag param
            final URI nonexistentFileUri = repo.resolve(getBaseUri(), "file4.txt");
            assertFalse(repo.exists(nonexistentFileUri));
            repo.delete(getBaseUri(), Lists.newArrayList("file4.txt"), IGNORE_NONEXISTENT);
            expectThrows(IOException.class, () -> {
               repo.delete(getBaseUri(), Lists.newArrayList("file4.txt"), REPORT_NONEXISTENT);
            });

            // Delete existing files individually and in 'bulk'
            repo.delete(getBaseUri(), Lists.newArrayList("file1.txt"), REPORT_NONEXISTENT);
            repo.delete(getBaseUri(), Lists.newArrayList("file2.txt", "file3.txt"), REPORT_NONEXISTENT);
            assertFalse(repo.exists(file1Uri));
            assertFalse(repo.exists(file2Uri));
            assertFalse(repo.exists(file3Uri));
        }
    }

    @Test
    public void testCanListFullOrEmptyDirectories() throws Exception {
        try (BackupRepository repo = getRepository()) {
            final URI rootUri = repo.resolve(getBaseUri(), "containsOtherDirs");
            final URI otherDir1Uri = repo.resolve(rootUri, "otherDir1");
            final URI otherDir2Uri = repo.resolve(rootUri, "otherDir2");
            final URI otherDir3Uri = repo.resolve(rootUri, "otherDir3");
            final URI file1Uri = repo.resolve(otherDir3Uri, "file1.txt");
            final URI file2Uri = repo.resolve(otherDir3Uri, "file2.txt");
            repo.createDirectory(rootUri);
            repo.createDirectory(otherDir1Uri);
            repo.createDirectory(otherDir2Uri);
            repo.createDirectory(otherDir3Uri);
            addFile(repo, file1Uri);
            addFile(repo, file2Uri);

            final List<String> rootChildren = Lists.newArrayList(repo.listAll(rootUri));
            assertEquals(3, rootChildren.size());
            assertTrue(rootChildren.contains("otherDir1"));
            assertTrue(rootChildren.contains("otherDir2"));
            assertTrue(rootChildren.contains("otherDir3"));

            final String[] otherDir2Children = repo.listAll(otherDir2Uri);
            assertEquals(0, otherDir2Children.length);

            final List<String> otherDir3Children = Lists.newArrayList(repo.listAll(otherDir3Uri));
            assertEquals(2, otherDir3Children.size());
            assertTrue(otherDir3Children.contains("file1.txt"));
            assertTrue(otherDir3Children.contains("file2.txt"));
        }
    }

    @Test
    public void testDirectoryExistsWithDirectoryUri() throws Exception {
        try (BackupRepository repo = getRepository()) {
            repo.createDirectory(getBaseUri());
            assertTrue(repo.exists(repo.createDirectoryURI(getBaseUri().toString())));
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
