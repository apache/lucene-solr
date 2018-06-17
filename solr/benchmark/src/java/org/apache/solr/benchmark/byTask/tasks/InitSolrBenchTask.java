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
package org.apache.solr.benchmark.byTask.tasks;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.tasks.PerfTask;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.solr.benchmark.byTask.util.StreamEater;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;

public class InitSolrBenchTask extends PerfTask {

  private Config config;
  private PerfRunData runData;

  public InitSolrBenchTask(PerfRunData runData) {
    super(runData);
    this.config = runData.getConfig();
    this.runData = runData;
  }

  @Override
  protected String getLogMessage(int recsCount) {
    return "init solr bench done";
  }
  
  @Override
  public int doLogic() throws Exception {
    String solrServerUrl = config.get("solr.url", null);
    boolean internalSolrServer = false;
    if (solrServerUrl == null) {
      solrServerUrl = "http://127.0.0.1:8901/solr";
      internalSolrServer  = true;
    }
    
    String solrCollection = config.get("solr.collection", "gettingstarted");
    String solrServerClass = config.get("solr.server", Http2SolrClient.class.getName());
    
    SolrClient solrServer = null;
    if (solrServerClass != null) {
      Class<?> clazz = this.getClass().getClassLoader()
          .loadClass(solrServerClass);
      
      if (clazz == CloudSolrClient.class) {
        String zkHost = config.get("solr.zkhost", null);
        if (zkHost == null) {
          throw new RuntimeException(
              "CloudSolrServer is used with no solr.zkhost specified");
        }
        zkHost = zkHost.replaceAll("\\|", ":");
        System.out.println("------------> new CloudSolrServer with ZkHost:"
            + zkHost);
        
        Constructor[] cons = clazz.getConstructors();
        for (Constructor con : cons) {
          Class[] types = con.getParameterTypes();
          if (types.length == 1 && types[0] == String.class) {
            solrServer  = (SolrClient) con.newInstance(zkHost);
          }
        }
        ((CloudSolrClient) solrServer).setDefaultCollection(solrCollection);
      } else if (clazz == Http2SolrClient.class) {
        System.out.println("------------> new HttpSolrServer with URL:"
            + solrServerUrl + "/" + solrCollection);
        Constructor[] cons = clazz.getConstructors();
        for (Constructor con : cons) {
          Class[] types = con.getParameterTypes();
          if (types.length == 1 && types[0] == String.class) {
            solrServer = (SolrClient) con.newInstance(solrServerUrl + "/"
                + solrCollection);
            break;
          }
        }
      } else if (clazz == ConcurrentUpdateSolrClient.class) {
        System.out.println("------------> new ConcurrentUpdateSolrServer with URL:"
            + solrServerUrl + "/" + solrCollection);
        Constructor[] cons = clazz.getConstructors();
        for (Constructor con : cons) {
          Class[] types = con.getParameterTypes();
          if (types.length == 3 && clazz == ConcurrentUpdateSolrClient.class) {
            int queueSize = config.get("solr.streaming.server.queue.size", 100);
            int threadCount = config
                .get("solr.streaming.server.threadcount", 2);
            solrServer = (SolrClient) con.newInstance(solrServerUrl + "/"
                + solrCollection, queueSize, threadCount);
          }
        }
      }
     
      
      if (solrServer == null) {
        throw new RuntimeException("Could not understand solr.server config:"
            + solrServerClass);
        
      }
    }
    
    String configDir = config.get("solr.config.dir", null);
    
    if (configDir == null && internalSolrServer) {
      configDir = "../server/solr/configsets/basic_configs/conf";
    }
    
    String configsHome = config.get("solr.configs.home", null);
    
    if (configDir != null && configsHome != null) {
      System.out.println("------------> solr.configs.home: "
          + new File(configsHome).getAbsolutePath());
      String solrConfig = config.get("solr.config", null);
      String schema = config.get("solr.schema", null);
      
      boolean copied = false;
      
      if (solrConfig != null) {
        File solrConfigFile = new File(configsHome, solrConfig);
        if (solrConfigFile.exists() && solrConfigFile.canRead()) {
          copy(solrConfigFile, new File(configDir, "solrconfig.xml"));
          copied = true;
        } else {
          throw new RuntimeException("Could not find or read:" + solrConfigFile);
        }
      }
      if (schema != null) {
        
        File schemaFile = new File(configsHome, schema);
        if (schemaFile.exists() && schemaFile.canRead()) {
          System.out.println("------------> using schema: " + schema);
          copy(schemaFile, new File(configDir, "managed-schema"));
          copied = true;
        } else {
          throw new RuntimeException("Could not find or read:" + schemaFile);
        }
      }
      
      if (copied && !internalSolrServer) {
        // TODO: check response
        try (Http2SolrClient client = new Http2SolrClient.Builder(solrServerUrl).build()) {
          CoreAdminResponse result = CoreAdminRequest.reloadCore(solrCollection, client);
        }
      }
    }
    
    runData.setPerfObject("solr.client", solrServer);
    
    return 1;
  }
  
  @Override
  public void close() {

  }
  
  /**
   * Set the params (docSize only)
   * @param params docSize, or 0 for no limit.
   */
  @Override
  public void setParams(String params) {
    super.setParams(params);
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
   */
  @Override
  public boolean supportsParams() {
    return true;
  }
  
  
  // closes streams for you
  private static void copy(File in, File out) throws IOException {
    System.out.println("------------> copying: " + in + " to " + out.getAbsolutePath());
    FileOutputStream os = new FileOutputStream(out);
    FileInputStream is = new FileInputStream(in);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os,
        "UTF-8"));
    BufferedReader reader = new BufferedReader(new InputStreamReader(is,
        "UTF-8"));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        writer.write(line + System.getProperty("line.separator"));
      }
    } finally {
      reader.close();
      writer.close();
    }
  }
  
  public static void runCmd(List<String> cmds, String workingDir, boolean log, boolean wait) throws IOException,
      InterruptedException {

    if (log) System.out.println(cmds);

    ProcessBuilder pb = new ProcessBuilder(cmds);
    if (workingDir != null) {
      pb.directory(new File(workingDir));
    }

    Process p = pb.start();

    OutputStream stdout = null;
    OutputStream stderr = null;

    if (log) {
      stdout = System.out;
      stderr = System.err;
    }

    StreamEater se = new StreamEater(p.getInputStream(), stdout);
    se.start();
    StreamEater se2 = new StreamEater(p.getErrorStream(), stderr);
    se2.start();

    if (wait) {
      se.join();
      se2.join();
    }
  }
  
}
