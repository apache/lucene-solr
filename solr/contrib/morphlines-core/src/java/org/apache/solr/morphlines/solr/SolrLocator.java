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
package org.apache.solr.morphlines.solr;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.Objects;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.SystemIdResolver;
import org.apache.zookeeper.KeeperException;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.base.Configs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Set of configuration parameters that identify the location and schema of a Solr server or
 * SolrCloud; Based on this information this class can return the schema and a corresponding
 * {@link DocumentLoader}.
 */
public class SolrLocator {
  
  private Config config;
  private MorphlineContext context;
  private String collectionName;
  private String zkHost;
  private String solrUrl;
  private String solrHomeDir;
  private int batchSize = 1000;
  
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected SolrLocator(MorphlineContext context) {
    this.context = Objects.requireNonNull(context);
  }

  public SolrLocator(Config config, MorphlineContext context) {
    this(context);
    this.config = config;
    Configs configs = new Configs();
    collectionName = configs.getString(config, "collection", null);
    zkHost = configs.getString(config, "zkHost", null);
    solrHomeDir = configs.getString(config, "solrHomeDir", null);
    solrUrl = configs.getString(config, "solrUrl", null);    
    batchSize = configs.getInt(config, "batchSize", batchSize);
    LOG.trace("Constructed solrLocator: {}", this);
    configs.validateArguments(config);
  }
  
  public DocumentLoader getLoader() {
    if (context instanceof SolrMorphlineContext) {
      DocumentLoader loader = ((SolrMorphlineContext)context).getDocumentLoader();
      if (loader != null) {
        return loader;
      }
    }
    
    if (zkHost != null && zkHost.length() > 0) {
      if (collectionName == null || collectionName.length() == 0) {
        throw new MorphlineCompilationException("Parameter 'zkHost' requires that you also pass parameter 'collection'", config);
      }
      CloudSolrClient cloudSolrClient = new Builder()
          .withZkHost(zkHost)
          .build();
      cloudSolrClient.setDefaultCollection(collectionName);
      cloudSolrClient.connect();
      return new SolrClientDocumentLoader(cloudSolrClient, batchSize);
    } else {
      if (solrUrl == null || solrUrl.length() == 0) {
        throw new MorphlineCompilationException("Missing parameter 'solrUrl'", config);
      }
      int solrServerNumThreads = 2;
      int solrServerQueueLength = solrServerNumThreads;
      SolrClient server = new SafeConcurrentUpdateSolrClient(solrUrl, solrServerQueueLength, solrServerNumThreads);
      // SolrServer server = new HttpSolrServer(solrServerUrl);
      // SolrServer server = new ConcurrentUpdateSolrServer(solrServerUrl, solrServerQueueLength, solrServerNumThreads);
      // server.setParser(new XMLResponseParser()); // binary parser is used by default
      return new SolrClientDocumentLoader(server, batchSize);
    }
  }

  public IndexSchema getIndexSchema() {
    if (context instanceof SolrMorphlineContext) {    
      IndexSchema schema = ((SolrMorphlineContext)context).getIndexSchema();
      if (schema != null) {
        validateSchema(schema);
        return schema;
      }
    }

    File downloadedSolrHomeDir = null;
    try {
      // If solrHomeDir isn't defined and zkHost and collectionName are defined 
      // then download schema.xml and solrconfig.xml, etc from zk and use that as solrHomeDir
      String mySolrHomeDir = solrHomeDir;
      if (solrHomeDir == null || solrHomeDir.length() == 0) {
        if (zkHost == null || zkHost.length() == 0) {
          // TODO: implement download from solrUrl if specified
          throw new MorphlineCompilationException(
              "Downloading a Solr schema requires either parameter 'solrHomeDir' or parameters 'zkHost' and 'collection'",
              config);
        }
        if (collectionName == null || collectionName.length() == 0) {
          throw new MorphlineCompilationException(
              "Parameter 'zkHost' requires that you also pass parameter 'collection'", config);
        }
        ZooKeeperDownloader zki = new ZooKeeperDownloader();
        SolrZkClient zkClient = zki.getZkClient(zkHost);
        try {
          String configName = zki.readConfigName(zkClient, collectionName);
          downloadedSolrHomeDir = Files.createTempDir();
          downloadedSolrHomeDir = zki.downloadConfigDir(zkClient, configName, downloadedSolrHomeDir);
          mySolrHomeDir = downloadedSolrHomeDir.getAbsolutePath();
        } catch (KeeperException | InterruptedException | IOException e) {
          throw new MorphlineCompilationException("Cannot download schema.xml from ZooKeeper", config, e);
        } finally {
          zkClient.close();
        }
      }
      
      LOG.debug("SolrLocator loading IndexSchema from dir {}", mySolrHomeDir);
      try {
        SolrResourceLoader loader = new SolrResourceLoader(Paths.get(mySolrHomeDir));
        SolrConfig solrConfig = new SolrConfig(loader, "solrconfig.xml", null);
        InputSource is = new InputSource(loader.openSchema("schema.xml"));
        is.setSystemId(SystemIdResolver.createSystemIdFromResourceName("schema.xml"));
        
        IndexSchema schema = new IndexSchema(solrConfig, "schema.xml", is);
        validateSchema(schema);
        return schema;
      } catch (ParserConfigurationException | IOException | SAXException e) {
        throw new MorphlineRuntimeException(e);
      }
    } finally {
      if (downloadedSolrHomeDir != null) {
        try {
          FileUtils.deleteDirectory(downloadedSolrHomeDir);
        } catch (IOException e) {
          LOG.warn("Cannot delete tmp directory", e);
        }
      }
    }
  }
  
  private void validateSchema(IndexSchema schema) {
    if (schema.getUniqueKeyField() == null) {
      throw new MorphlineCompilationException("Solr schema.xml is missing unique key field", config);
    }
    if (!schema.getUniqueKeyField().isRequired()) {
      throw new MorphlineCompilationException("Solr schema.xml must contain a required unique key field", config);
    }
  }
  
  @Override
  public String toString() {
    return toConfig(null).root().render(ConfigRenderOptions.concise());
  }
  
  public Config toConfig(String key) {
    String json = "";
    if (key != null) {
      json = toJson(key) + " : ";
    }
    json +=  
        "{" +
        " collection : " + toJson(collectionName) + ", " +
        " zkHost : " + toJson(zkHost) + ", " +
        " solrUrl : " + toJson(solrUrl) + ", " +
        " solrHomeDir : " + toJson(solrHomeDir) + ", " +
        " batchSize : " + toJson(batchSize) + " " +
        "}";
    return ConfigFactory.parseString(json);
  }
  
  private String toJson(Object key) {
    String str = key == null ? "" : key.toString();
    str = ConfigUtil.quoteString(str);
    return str;
  }

  public String getCollectionName() {
    return this.collectionName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }

  public String getZkHost() {
    return this.zkHost;
  }

  public void setZkHost(String zkHost) {
    this.zkHost = zkHost;
  }

  public String getSolrHomeDir() {
    return this.solrHomeDir;
  }

  public void setSolrHomeDir(String solrHomeDir) {
    this.solrHomeDir = solrHomeDir;
  }

  public String getServerUrl() {
    return this.solrUrl;
  }

  public void setServerUrl(String solrUrl) {
    this.solrUrl = solrUrl;
  }

  public int getBatchSize() {
    return this.batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }
  
}
