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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.solr.common.SolrException;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.quorum.flexible.QuorumHierarchical;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.slf4j.LoggerFactory;


public class SolrZkServer {
  static org.slf4j.Logger log = LoggerFactory.getLogger(SolrZkServer.class);
  
  String zkRun;
  String zkHost;
  String solrHome;
  String solrPort;
  Properties props;
  SolrZkServerProps zkProps;

  private Thread zkThread;  // the thread running a zookeeper server, only if zkRun is set

  public SolrZkServer(String zkRun, String zkHost, String solrHome, String solrPort) {
    this.zkRun = zkRun;
    this.zkHost = zkHost;
    this.solrHome = solrHome;
    this.solrPort = solrPort;
  }

  public String getClientString() {
    if (zkHost != null) return zkHost;
    
    if (zkProps == null) return null;

    // if the string wasn't passed as zkHost, then use the standalone server we started
    if (zkRun == null) return null;
    return "localhost:" + zkProps.getClientPortAddress().getPort();
  }

  public void parseConfig() {
    if (zkProps == null) {
      zkProps = new SolrZkServerProps();
      // set default data dir
      // TODO: use something based on IP+port???  support ensemble all from same solr home?
      zkProps.setDataDir(solrHome + '/' + "zoo_data");
      zkProps.zkRun = zkRun;
      zkProps.solrPort = solrPort;
    }
    
    try {
      props = SolrZkServerProps.getProperties(solrHome + '/' + "zoo.cfg");
      SolrZkServerProps.injectServers(props, zkRun, zkHost);
      zkProps.parseProperties(props);
      if (zkProps.getClientPortAddress() == null) {
        zkProps.setClientPort(Integer.parseInt(solrPort)+1000);
      }
    } catch (QuorumPeerConfig.ConfigException e) {
      if (zkRun != null)
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (IOException e) {
      if (zkRun != null)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public Map<Long, QuorumPeer.QuorumServer> getServers() {
    return zkProps.getServers();
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
      log.info("STARTING EMBEDDED ENSEMBLE ZOOKEEPER SERVER at port " + zkProps.getClientPortAddress().getPort());
    } else {
      log.info("STARTING EMBEDDED STANDALONE ZOOKEEPER SERVER at port " + zkProps.getClientPortAddress().getPort());
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
    zkThread.interrupt();
  }
}




// Allows us to set a default for the data dir before parsing
// zoo.cfg (which validates that there is a dataDir)
class SolrZkServerProps extends QuorumPeerConfig {
  protected static org.slf4j.Logger LOG = LoggerFactory.getLogger(QuorumPeerConfig.class);

  String solrPort; // port that Solr is listening on
  String zkRun;

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


  // Adds server.x if they don't exist, based on zkHost if it does exist.
  // Given zkHost=localhost:1111,localhost:2222 this will inject
  // server.0=localhost:1112:1113
  // server.1=localhost:2223:2224
  public static void injectServers(Properties props, String zkRun, String zkHost) {

    // if clientPort not already set, use zkRun
    if (zkRun != null && props.getProperty("clientPort")==null) {
      int portIdx = zkRun.lastIndexOf(':');
      if (portIdx > 0) {
        String portStr = zkRun.substring(portIdx+1);
        props.setProperty("clientPort", portStr);
      }
    }

    boolean hasServers = hasServers(props);

    if (!hasServers && zkHost != null) {
      int alg = Integer.parseInt(props.getProperty("electionAlg","3").trim());
      String[] hosts = zkHost.split(",");
      int serverNum = 0;
      for (String hostAndPort : hosts) {
        hostAndPort = hostAndPort.trim();
        int portIdx = hostAndPort.lastIndexOf(':');
        String clientPortStr = hostAndPort.substring(portIdx+1);
        int clientPort = Integer.parseInt(clientPortStr);
        String host = hostAndPort.substring(0,portIdx);

        String serverStr = host + ':' + (clientPort+1);
        // zk leader election algorithms other than 0 need an extra port for leader election.
        if (alg != 0) {
          serverStr = serverStr + ':' + (clientPort+2);
        }

        props.setProperty("server."+serverNum, serverStr);
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
    if (zkRun == null && solrPort == null) return null;

    Map<Long, QuorumPeer.QuorumServer> slist = getServers();

    String myHost = "localhost";
    InetSocketAddress thisAddr = null;

    if (zkRun != null && zkRun.length()>0) {
      String parts[] = zkRun.split(":");
      myHost = parts[0];
      thisAddr = new InetSocketAddress(myHost, Integer.parseInt(parts[1]) + 1);
    } else {
      // default to localhost:<solrPort+1001>
      thisAddr = new InetSocketAddress(myHost, Integer.parseInt(solrPort)+1001);
    }


    // first try a straight match by host
    Long me = null;
    boolean multiple = false;
    int port = 0;
    for (QuorumPeer.QuorumServer server : slist.values()) {
      if (server.addr.getHostName().equals(myHost)) {
        multiple = me!=null;
        me = server.id;
        port = server.addr.getPort();
      }
    }

    if (!multiple) {
      // only one host matched... assume it's me.
      setClientPort(port - 1);
      return me;
    }

    if (me == null) {
      // no hosts matched.
      return null;
    }


    // multiple matches... try to figure out by port.
    for (QuorumPeer.QuorumServer server : slist.values()) {
      if (server.addr.equals(thisAddr)) {
        if (clientPortAddress == null || clientPortAddress.getPort() <= 0)
          setClientPort(server.addr.getPort() - 1);
        return server.id;
      }
    }

    return null;
  }



  public void setDataDir(String dataDir) {
    this.dataDir = dataDir;
  }

  public void setClientPort(int clientPort) {
    if (clientPortAddress != null) {
      try {
        this.clientPortAddress = new InetSocketAddress(
                InetAddress.getByName(clientPortAddress.getHostName()), clientPort);
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    } else {
      this.clientPortAddress = new InetSocketAddress(clientPort);
    }
  }


  // NOTE: copied from ZooKeeper 3.2
  /**
   * Parse config from a Properties.
   * @param zkProp Properties to parse from.
   * @throws java.io.IOException
   * @throws ConfigException
   */
  @Override
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
        setClientPort(Integer.parseInt(value));
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
        if (zkRun == null) return;
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
