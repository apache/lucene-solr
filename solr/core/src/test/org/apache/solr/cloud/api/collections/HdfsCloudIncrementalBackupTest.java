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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@LuceneTestCase.SuppressCodecs({"SimpleText"})
@ThreadLeakFilters(defaultFilters = true, filters = {
        BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
public class HdfsCloudIncrementalBackupTest extends AbstractIncrementalBackupTest{
    public static final String SOLR_XML = "<solr>\n" +
            "\n" +
            "  <str name=\"shareSchema\">${shareSchema:false}</str>\n" +
            "  <str name=\"configSetBaseDir\">${configSetBaseDir:configsets}</str>\n" +
            "  <str name=\"coreRootDirectory\">${coreRootDirectory:.}</str>\n" +
            "\n" +
            "  <shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\">\n" +
            "    <str name=\"urlScheme\">${urlScheme:}</str>\n" +
            "    <int name=\"socketTimeout\">${socketTimeout:90000}</int>\n" +
            "    <int name=\"connTimeout\">${connTimeout:15000}</int>\n" +
            "  </shardHandlerFactory>\n" +
            "\n" +
            "  <solrcloud>\n" +
            "    <str name=\"host\">127.0.0.1</str>\n" +
            "    <int name=\"hostPort\">${hostPort:8983}</int>\n" +
            "    <str name=\"hostContext\">${hostContext:solr}</str>\n" +
            "    <int name=\"zkClientTimeout\">${solr.zkclienttimeout:30000}</int>\n" +
            "    <bool name=\"genericCoreNodeNames\">${genericCoreNodeNames:true}</bool>\n" +
            "    <int name=\"leaderVoteWait\">10000</int>\n" +
            "    <int name=\"distribUpdateConnTimeout\">${distribUpdateConnTimeout:45000}</int>\n" +
            "    <int name=\"distribUpdateSoTimeout\">${distribUpdateSoTimeout:340000}</int>\n" +
            "  </solrcloud>\n" +
            "  \n" +
            "  <backup>\n" +
            "    <repository name=\"trackingBackupRepository\" default=\"true\" class=\"org.apache.solr.core.TrackingBackupRepository\"> \n" +
            "      <str name=\"delegateRepoName\">hdfs</str>\n" +
            "    </repository>\n" +
            "    <repository  name=\"hdfs\" class=\"org.apache.solr.core.backup.repository.HdfsBackupRepository\"> \n" +
            "      <str name=\"location\">${solr.hdfs.default.backup.path}</str>\n" +
            "      <str name=\"solr.hdfs.home\">${solr.hdfs.home:}</str>\n" +
            "      <str name=\"solr.hdfs.confdir\">${solr.hdfs.confdir:}</str>\n" +
            "    </repository>\n" +
            "  </backup>\n" +
            "  \n" +
            "</solr>\n";

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

        configureCluster(NUM_SHARDS)// nodes
                .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
                .withSolrXml(SOLR_XML)
                .configure();
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
    public String getCollectionNamePrefix() {
        return "hdfsbackuprestore";
    }

    @Override
    public String getBackupLocation() {
        return null;
    }
}
