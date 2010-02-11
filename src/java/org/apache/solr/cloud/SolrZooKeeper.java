package org.apache.solr.cloud;

import java.io.IOException;

import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class SolrZooKeeper extends ZooKeeper {

  public SolrZooKeeper(String connectString, int sessionTimeout, Watcher watcher)
      throws IOException {
    super(connectString, sessionTimeout, watcher);
    // TODO Auto-generated constructor stub
  }
  
  protected ClientCnxn getConnection() {
    return cnxn;
  }

}
