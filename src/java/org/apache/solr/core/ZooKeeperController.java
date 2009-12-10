package org.apache.solr.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.common.SolrException;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.ZooPut;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * Handle ZooKeeper interactions.
 */
public class ZooKeeperController implements Watcher {
  private static final String CONFIGS_NODE = "configs";

  private static Logger log = LoggerFactory
      .getLogger(ZooKeeperController.class);

  private ZooKeeper keeper;

  private String configName;

  private String collectionName;

  /**
   * @param zookeeperHost ZooKeeper host service
   * @param zkSolrPathPrefix Solr ZooKeeper node (default is /solr)
   */
  public ZooKeeperController(String zookeeperHost, String collection) {


    this.collectionName = collection;
    try {
      keeper = new ZooKeeper(zookeeperHost, 10000, this);
      loadConfigPath();
      register();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Can't create ZooKeeper instance", e);
    }

  }

  // nocommit: fooling around
  private void register() throws IOException {
    try {
      String host = InetAddress.getLocalHost().getHostName();
      ZooPut zooPut = new ZooPut(keeper);
      zooPut.makePath("/hosts/" + host);
    } catch (UnknownHostException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Could not determine IP of host", e);
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "ZooKeeper Exception", e);
    } catch (InterruptedException e) {
      // nocommit: handle
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
   */
  public void process(WatchedEvent event) {
    // nocommit
    System.out.println("ZooKeeper Event:" + event);
  }

  private void loadConfigPath() {
    // nocommit: load all config at once or organize differently
    try {
      String path = "/collections/" + collectionName;
      // nocommit
      System.out.println("look for collection config:" + path);
      List<String> children = keeper.getChildren(path, null);
      for (String node : children) {
        // nocommit
        System.out.println("check child:" + node);
        // nocommit: do we actually want to handle settings in the node name?
        if (node.startsWith("config=")) {
          configName = node.substring(node.indexOf("=") + 1);
          // nocommit
          System.out.println("config:" + configName);
        }
      }
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "ZooKeeper Exception", e);
    } catch (InterruptedException e) {
      //nocommit
    }
    if (configName == null) {
      throw new IllegalStateException("no config specified for collection:"
          + collectionName);
    }
  }

  /**
   * Load IndexSchema from ZooKeeper.
   * 
   * @param resourceLoader
   * @param schemaName
   * @param config
   * @return
   */
  public IndexSchema getSchema(String schemaName, SolrConfig config,
      SolrResourceLoader resourceLoader) {
    byte[] configBytes = getFile("/" + CONFIGS_NODE + "/"
        + configName, schemaName);
    InputStream is = new ByteArrayInputStream(configBytes);
    IndexSchema schema = new IndexSchema(config, schemaName, is);
    return schema;
  }

  /**
   * Load SolrConfig from ZooKeeper.
   * 
   * @param resourceLoader
   * @param solrConfigName
   * @return
   * @throws IOException
   * @throws ParserConfigurationException
   * @throws SAXException
   */
  public SolrConfig getConfig(String solrConfigName,
      SolrResourceLoader resourceLoader) throws IOException,
      ParserConfigurationException, SAXException {
    byte[] config = getFile("/" + CONFIGS_NODE + "/"
        + configName, solrConfigName);
    InputStream is = new ByteArrayInputStream(config);
    SolrConfig cfg = solrConfigName == null ? new SolrConfig(resourceLoader,
        SolrConfig.DEFAULT_CONF_FILE, is) : new SolrConfig(resourceLoader,
        solrConfigName, is);

    return cfg;
  }

  public boolean exists(String path) {
    Object exists = null;
    try {
      exists = keeper.exists(path, null);
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "ZooKeeper Exception", e);
    } catch (InterruptedException e) {
      // nocommit: handle
    }
    return exists != null;
  }

  public byte[] getFile(String path, String file) {
    byte[] bytes = null;
    String configPath = path + "/" + file;
    try {
      log.info("Reading " + file + " from zookeeper at " + configPath);
      bytes = keeper.getData(configPath, false, null);
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "ZooKeeper Exception", e);
    } catch (InterruptedException e) {
      // nocommit: handle
    }

    return bytes;
  }

  public void close() {
    try {
      keeper.close();
    } catch (InterruptedException e) {
      // nocommit: handle
    }
  }
}
