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

package org.apache.solr.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.util.ExternalPaths;
import org.apache.zookeeper.data.Stat;
import org.junit.Ignore;

import static org.apache.solr.core.TestConfigSets.solrxml;

public class TestConfLoadPerf extends SolrTestCaseJ4 {

  @Ignore
  @SuppressForbidden(reason = "Needed to provide time for tests.")
  public void testPerf() throws Exception{
    String sourceHome = ExternalPaths.SOURCE_HOME;
    File configSetDir = new File(sourceHome, "server/solr/configsets/sample_techproducts_configs/conf");

    String configSetsBaseDir = TEST_PATH().resolve("configsets").toString();


    File file = new File(configSetDir, "solrconfig.xml");
    byte[]  b  = new byte[(int) file.length()];
    new FileInputStream(file).read(b);

    Path testDirectory = createTempDir();

    System.setProperty("configsets", configSetsBaseDir);

    CoreContainer container = new CoreContainer(SolrXmlConfig.fromString(testDirectory, solrxml));
    container.load();
    container.shutdown();

    SolrResourceLoader srl = new SolrResourceLoader("temp", Collections.emptyList(), container.solrHome, container.getResourceLoader().classLoader){

      @Override
      public CoreContainer getCoreContainer() {
        return container;
      }

      @Override
      public InputStream openResource(String resource) throws IOException {
        if(resource.equals("solrconfig.xml")) {
          Stat stat = new Stat();
          stat.setVersion(1);
          return new ZkSolrResourceLoader.ZkByteArrayInputStream(b, file.getAbsolutePath(), stat);
        } else {
          throw new FileNotFoundException(resource);
        }

      }
    };
    System.gc();
    long heapSize = Runtime.getRuntime().totalMemory();
    List<SolrConfig> allConfigs = new ArrayList<>();
    long startTime =  System.currentTimeMillis();
    for(int i=0;i<100;i++) {
      allConfigs.add(SolrConfig.readFromResourceLoader(srl, "solrconfig.xml", true, null));

    }
    System.gc();
    System.out.println("TIME_TAKEN : "+(System.currentTimeMillis()-startTime));
    System.out.println("HEAP_SIZE : "+((Runtime.getRuntime().totalMemory()-heapSize)/(1024)));
  }
}
