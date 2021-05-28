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

package org.apache.solr.core.backup.repository;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.cloud.api.collections.AbstractBackupRepositoryTest;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.HdfsDirectoryFactory;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@LuceneTestCase.SuppressCodecs({"SimpleText"}) // Backups do checksum validation against a footer value not present in 'SimpleText'
@ThreadLeakFilters(defaultFilters = true, filters = {
        BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
public class HdfsBackupRepositoryIntegrationTest extends AbstractBackupRepositoryTest {
    private static MiniDFSCluster dfsCluster;
    private static String hdfsUri;
    private static FileSystem fs;

    @BeforeClass
    public static void setupClass() throws Exception {
        dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());
        hdfsUri = HdfsTestUtil.getURI(dfsCluster);
        try {
            URI uri = new URI(hdfsUri);
            Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
            fs = FileSystem.get(uri, conf);

            if (fs instanceof DistributedFileSystem) {
                // Make sure dfs is not in safe mode
                while (((DistributedFileSystem) fs).setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET, true)) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                        // continue
                    }
                }
            }

            fs.mkdirs(new org.apache.hadoop.fs.Path("/backup"));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }

        System.setProperty("solr.hdfs.default.backup.path", "/backup");
        System.setProperty("solr.hdfs.home", hdfsUri + "/solr");
        useFactory("solr.StandardDirectoryFactory");
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        IOUtils.closeQuietly(fs);
        fs = null;
        try {
            HdfsTestUtil.teardownClass(dfsCluster);
        } finally {
            dfsCluster = null;
            System.clearProperty("solr.hdfs.home");
            System.clearProperty("solr.hdfs.default.backup.path");
            System.clearProperty("test.build.data");
            System.clearProperty("test.cache.data");
        }
    }

    @Override
    protected BackupRepository getRepository() {
        HdfsBackupRepository repository = new HdfsBackupRepository();
        repository.init(getBaseBackupRepositoryConfiguration());
        return repository;
    }

    @Override
    protected URI getBaseUri() throws URISyntaxException {
        return new URI(hdfsUri+"/solr/tmp");
    }

    @Override
    protected NamedList<Object> getBaseBackupRepositoryConfiguration() {
        NamedList<Object> config = new NamedList<>();
        config.add(HdfsDirectoryFactory.HDFS_HOME, hdfsUri + "/solr");
        return config;
    }
}
