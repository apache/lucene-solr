package org.apache.solr.cloud;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.quorum.flexible.QuorumHierarchical;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.solr.common.SolrException;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Map;
import java.io.*;
import java.util.Map.Entry;
import java.net.InetSocketAddress;


public class SolrZkServer {
  static org.slf4j.Logger log = LoggerFactory.getLogger(SolrZkServer.class);
  
  String zkRun;
  String zkHost;
  String solrHome;
  String solrPort;
  Properties props;
  SolrZkServerProps zkProps;

  private Thread zkThread;  // the thread running a zookeeper server, only if zkRun is set

  public SolrZkServer(String runZk, String zkHost, String solrHome, String solrPort) {
    this.zkRun = runZk;
    this.zkHost = zkHost;
    this.solrHome = solrHome;
    this.solrPort = solrPort;
  }

  public String getClientString() {
    if (props==null) return null;

    StringBuilder result = new StringBuilder();
    for (Entry<Object, Object> entry : props.entrySet()) {
      String key = entry.getKey().toString().trim();
      String value = entry.getValue().toString().trim();
      if (key.startsWith("server.")) {
        int first = value.indexOf(':');
        int second = value.indexOf(':', first+1);
        String host = value.substring(0, second>0 ? second : first);
        if (result.length() > 0)
          result.append(',');
        result.append(host);
      }
    }
    return result.toString();
  }

  public void parseConfig() {
    if (zkProps == null) {
      zkProps = new SolrZkServerProps();
      // set default data dir
      zkProps.setDataDir(solrHome + '/' + "zoo_data");
      zkProps.runZk = zkRun;
      zkProps.solrPort = solrPort;
    }
    
    try {
      props = SolrZkServerProps.getProperties(solrHome + '/' + "zoo.cfg");
      SolrZkServerProps.injectServers(props, zkHost);
      zkProps.parseProperties(props);
    } catch (QuorumPeerConfig.ConfigException e) {
      if (zkRun != null)
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (IOException e) {
      if (zkRun != null)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void start() {
    if (zkRun == null) return;

    zkThread = new Thread() {
      @Override
      public void run() {
        try {
          if (zkProps.getServers().size() > 1) {
            QuorumPeerMain zkServer = new QuorumPeerMain();
            zkServer.runFromConfig(zkProps);
          } else {
            ServerConfig sc = new ServerConfig();
            sc.readFrom(zkProps);
            ZooKeeperServerMain zkServer = new ZooKeeperServerMain();
            zkServer.runFromConfig(sc);
          }
          log.info("ZooKeeper Server exited.");
        } catch (Throwable e) {
          log.error("ZooKeeper Server ERROR", e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      }
    };

    if (zkProps.getServers().size() > 1) {
      log.info("STARTING EMBEDDED ENSEMBLE ZOOKEEPER SERVER");      
    } else {
      log.info("STARTING EMBEDDED STANDALONE ZOOKEEPER SERVER");
    }

    zkThread.setDaemon(true);
    zkThread.start();
    try {
      Thread.sleep(500); // pause for ZooKeeper to start
    } catch (Exception e) {
      log.error("STARTING ZOOKEEPER", e);
    }
  }

  public void stop() {
    if (zkRun == null) return;
    // TODO: how to do an orderly shutdown?
    zkThread.interrupt();
  }
}




// Allows us to set a default for the data dir before parsing
// zoo.cfg (which validates that there is a dataDir)
class SolrZkServerProps extends QuorumPeerConfig {
  protected static org.slf4j.Logger LOG = LoggerFactory.getLogger(QuorumPeerConfig.class);

  String solrPort; // port that Solr is listening on
  String runZk;    // the zkRun param
  Properties sourceProps;

  /**
   * Parse a ZooKeeper configuration file
   * @param path the patch of the configuration file
   * @throws ConfigException error processing configuration
   */
  public static Properties getProperties(String path) throws ConfigException {
    File configFile = new File(path);

    LOG.info("Reading configuration from: " + configFile);

    try {
      if (!configFile.exists()) {
        throw new IllegalArgumentException(configFile.toString()
            + " file is missing");
      }

      Properties cfg = new Properties();
      FileInputStream in = new FileInputStream(configFile);
      try {
        cfg.load(in);
      } finally {
        in.close();
      }

      return cfg;

    } catch (IOException e) {
      throw new ConfigException("Error processing " + path, e);
    } catch (IllegalArgumentException e) {
      throw new ConfigException("Error processing " + path, e);
    }
  }


  // adds server.x if they don't exist, based on zkHost if it does exist
  public static void injectServers(Properties props, String zkHost) {
    boolean hasServers = hasServers(props);

    if (!hasServers && zkHost != null) {
      int alg = Integer.parseInt(props.getProperty("electionAlg","3").trim());
      String[] hosts = zkHost.split(",");
      int serverNum = 0;
      for (String host : hosts) {
        host = host.trim();
        // algorithms other than 0 need an extra port for leader election.
        if (alg != 0) {
          int port = Integer.parseInt(host.substring(host.indexOf(':')+1));
          host = host + ":" + (port+1);
        }
        props.setProperty("server."+serverNum, host);
        serverNum++;
      }
    }
  }

  public static boolean hasServers(Properties props) {
    for (Object key : props.keySet())
      if (((String)key).startsWith("server."))
        return true;
    return false;
  }

  // called by the modified version of parseProperties
  // when the myid file is missing.
  public Long getMySeverId() {
    if (runZk == null && solrPort == null) return null;

    InetSocketAddress thisAddr = null;

    if (runZk != null && runZk.length()>0) {
      String parts[] = runZk.split(":");
      thisAddr = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
    } else {
     // default to localhost:<solrPort+1000>
     thisAddr = new InetSocketAddress("localhost", Integer.parseInt(solrPort)+1000);
    }

    Map<Long, QuorumPeer.QuorumServer> slist = getServers();

    for (QuorumPeer.QuorumServer server : slist.values()) {
      if (server.addr.equals(thisAddr)) {
        LOG.info("I AM SERVER #" + server.id + " Addr=" + server.addr);
        return server.id;
      }
    }

    return null;
  }



  public void setDataDir(String dataDir) {
    this.dataDir = dataDir;
  }


  // NOTE: copied from ZooKeeper 3.2
  /**
   * Parse config from a Properties.
   * @param zkProp Properties to parse from.
   * @throws java.io.IOException
   * @throws ConfigException
   */
  public void parseProperties(Properties zkProp)
      throws IOException, ConfigException {
    for (Entry<Object, Object> entry : zkProp.entrySet()) {
      String key = entry.getKey().toString().trim();
      String value = entry.getValue().toString().trim();
      if (key.equals("dataDir")) {
        dataDir = value;
      } else if (key.equals("dataLogDir")) {
        dataLogDir = value;
      } else if (key.equals("clientPort")) {
        clientPort = Integer.parseInt(value);
      } else if (key.equals("tickTime")) {
        tickTime = Integer.parseInt(value);
      } else if (key.equals("initLimit")) {
        initLimit = Integer.parseInt(value);
      } else if (key.equals("syncLimit")) {
        syncLimit = Integer.parseInt(value);
      } else if (key.equals("electionAlg")) {
        electionAlg = Integer.parseInt(value);
      } else if (key.equals("maxClientCnxns")) {
        maxClientCnxns = Integer.parseInt(value);
      } else if (key.startsWith("server.")) {
        int dot = key.indexOf('.');
        long sid = Long.parseLong(key.substring(dot + 1));
        String parts[] = value.split(":");
        if ((parts.length != 2) && (parts.length != 3)) {
          LOG.error(value
              + " does not have the form host:port or host:port:port");
        }
        InetSocketAddress addr = new InetSocketAddress(parts[0],
            Integer.parseInt(parts[1]));
        if (parts.length == 2) {
          servers.put(Long.valueOf(sid), new QuorumPeer.QuorumServer(sid, addr));
        } else if (parts.length == 3) {
          InetSocketAddress electionAddr = new InetSocketAddress(
              parts[0], Integer.parseInt(parts[2]));
          servers.put(Long.valueOf(sid), new QuorumPeer.QuorumServer(sid, addr,
              electionAddr));
        }
      } else if (key.startsWith("group")) {
        int dot = key.indexOf('.');
        long gid = Long.parseLong(key.substring(dot + 1));

        numGroups++;

        String parts[] = value.split(":");
        for(String s : parts){
          long sid = Long.parseLong(s);
          if(serverGroup.containsKey(sid))
            throw new ConfigException("Server " + sid + "is in multiple groups");
          else
            serverGroup.put(sid, gid);
        }

      } else if(key.startsWith("weight")) {
        int dot = key.indexOf('.');
        long sid = Long.parseLong(key.substring(dot + 1));
        serverWeight.put(sid, Long.parseLong(value));
      } else {
        System.setProperty("zookeeper." + key, value);
      }
    }
    if (dataDir == null) {
      throw new IllegalArgumentException("dataDir is not set");
    }
    if (dataLogDir == null) {
      dataLogDir = dataDir;
    } else {
      if (!new File(dataLogDir).isDirectory()) {
        throw new IllegalArgumentException("dataLogDir " + dataLogDir
            + " is missing.");
      }
    }
    if (clientPort == 0) {
      throw new IllegalArgumentException("clientPort is not set");
    }
    if (tickTime == 0) {
      throw new IllegalArgumentException("tickTime is not set");
    }
    if (servers.size() > 1) {
      if (initLimit == 0) {
        throw new IllegalArgumentException("initLimit is not set");
      }
      if (syncLimit == 0) {
        throw new IllegalArgumentException("syncLimit is not set");
      }
      /*
      * If using FLE, then every server requires a separate election
      * port.
      */
      if (electionAlg != 0) {
        for (QuorumPeer.QuorumServer s : servers.values()) {
          if (s.electionAddr == null)
            throw new IllegalArgumentException(
                "Missing election port for server: " + s.id);
        }
      }

      /*
      * Default of quorum config is majority
      */
      if(serverGroup.size() > 0){
        if(servers.size() != serverGroup.size())
          throw new ConfigException("Every server must be in exactly one group");
        /*
         * The deafult weight of a server is 1
         */
        for(QuorumPeer.QuorumServer s : servers.values()){
          if(!serverWeight.containsKey(s.id))
            serverWeight.put(s.id, (long) 1);
        }

        /*
                      * Set the quorumVerifier to be QuorumHierarchical
                      */
        quorumVerifier = new QuorumHierarchical(numGroups,
            serverWeight, serverGroup);
      } else {
        /*
                      * The default QuorumVerifier is QuorumMaj
                      */

        LOG.info("Defaulting to majority quorums");
        quorumVerifier = new QuorumMaj(servers.size());
      }

      File myIdFile = new File(dataDir, "myid");
      if (!myIdFile.exists()) {
        ///////////////// ADDED FOR SOLR //////
        Long myid = getMySeverId();
        if (myid != null) {
          serverId = myid;
          return;
        }
        if (runZk == null) return;
        //////////////// END ADDED FOR SOLR //////
        throw new IllegalArgumentException(myIdFile.toString()
            + " file is missing");
      }

      BufferedReader br = new BufferedReader(new FileReader(myIdFile));
      String myIdString;
      try {
        myIdString = br.readLine();
      } finally {
        br.close();
      }
      try {
        serverId = Long.parseLong(myIdString);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("serverid " + myIdString
            + " is not a number");
      }
    }
  }


}
