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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.management.JMException;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.SessionTracker.Session;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

public class ZkTestServer {

  protected final ZKServerMain zkServer = new ZKServerMain();

  private String zkDir;

  private int clientPort;

  private Thread zooThread;

  class ZKServerMain {

    private NIOServerCnxn.Factory cnxnFactory;
    private ZooKeeperServer zooKeeperServer;
    
    protected void initializeAndRun(String[] args) throws ConfigException,
        IOException {
      try {
        ManagedUtil.registerLog4jMBeans();
      } catch (JMException e) {

      }

      ServerConfig config = new ServerConfig();
      if (args.length == 1) {
        config.parse(args[0]);
      } else {
        config.parse(args);
      }

      runFromConfig(config);
    }

    /**
     * Run from a ServerConfig.
     * 
     * @param config ServerConfig to use.
     * @throws IOException
     */
    public void runFromConfig(ServerConfig config) throws IOException {
      try {
        // Note that this thread isn't going to be doing anything else,
        // so rather than spawning another thread, we will just call
        // run() in this thread.
        // create a file logger url from the command line args
        zooKeeperServer = new ZooKeeperServer();

        FileTxnSnapLog ftxn = new FileTxnSnapLog(new File(config
            .getDataLogDir()), new File(config.getDataDir()));
        zooKeeperServer.setTxnLogFactory(ftxn);
        zooKeeperServer.setTickTime(config.getTickTime());
        cnxnFactory = new NIOServerCnxn.Factory(config.getClientPortAddress(), config
            .getMaxClientCnxns());
        cnxnFactory.startup(zooKeeperServer);
        cnxnFactory.join();
        if (zooKeeperServer.isRunning()) {
          zooKeeperServer.shutdown();
        }
      } catch (InterruptedException e) {
      }
    }

    /**
     * Shutdown the serving instance
     * @throws IOException 
     */
    protected void shutdown() throws IOException {
      zooKeeperServer.shutdown();
      ZKDatabase zkDb = zooKeeperServer.getZKDatabase();
      if (zkDb != null) {
        zkDb.close();
      }
      waitForServerDown(getZkHost() + ":" + getPort(), 5000);
      cnxnFactory.shutdown();
    }

    public int getLocalPort() {
      if (cnxnFactory == null) {
        throw new IllegalStateException("A port has not yet been selected");
      }
      int port = cnxnFactory.getLocalPort();
      if (port == 0) {
        throw new IllegalStateException("A port has not yet been selected");
      }
      return port;
    }
  }

  public ZkTestServer(String zkDir) {
    this.zkDir = zkDir;
  }

  public ZkTestServer(String zkDir, int port) {
    this.zkDir = zkDir;
    this.clientPort = port;
  }

  public String getZkHost() {
    return "127.0.0.1:" + zkServer.getLocalPort();
  }

  public String getZkAddress() {
    return "127.0.0.1:" + zkServer.getLocalPort() + "/solr";
  }

  public int getPort() {
    return zkServer.getLocalPort();
  }
  
  public void expire(final long sessionId) {
    zkServer.zooKeeperServer.expire(new Session() {
      @Override
      public long getSessionId() {
        return sessionId;
      }
      @Override
      public int getTimeout() {
        return 4000;
      }});
  }

  public void run() throws InterruptedException {
    // we don't call super.setUp
    zooThread = new Thread() {
      
      @Override
      public void run() {
        ServerConfig config = new ServerConfig() {

          {
            setClientPort(ZkTestServer.this.clientPort);
            this.dataDir = zkDir;
            this.dataLogDir = zkDir;
            this.tickTime = 1500;
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
        };

        try {
          zkServer.runFromConfig(config);
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
    };

    zooThread.setDaemon(true);
    zooThread.start();

    int cnt = 0;
    int port = -1;
    try {
       port = getPort();
    } catch(IllegalStateException e) {
      
    }
    while (port < 1) {
      Thread.sleep(100);
      try {
        port = getPort();
      } catch(IllegalStateException e) {
        
      }
      if (cnt == 40) {
        throw new RuntimeException("Could not get the port for ZooKeeper server");
      }
      cnt++;
    }
  }

  @SuppressWarnings("deprecation")
  public void shutdown() throws IOException {
    SolrTestCaseJ4.ignoreException("java.nio.channels.ClosedChannelException");
    // TODO: this can log an exception while trying to unregister a JMX MBean
    try {
      zkServer.shutdown();
    } finally {
      SolrTestCaseJ4.resetExceptionIgnores();
    }
  }
 
  
  public static boolean waitForServerDown(String hp, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        HostPort hpobj = parseHostPortList(hp).get(0);
        send4LetterWord(hpobj.host, hpobj.port, "stat");
      } catch (IOException e) {
        return true;
      }
      
      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return false;
  }
  
  public static class HostPort {
    String host;
    int port;
    
    HostPort(String host, int port) {
      this.host = host;
      this.port = port;
    }
  }
  
  /**
   * Send the 4letterword
   * @param host the destination host
   * @param port the destination port
   * @param cmd the 4letterword
   * @return
   * @throws IOException
   */
  public static String send4LetterWord(String host, int port, String cmd)
      throws IOException
  {

      Socket sock = new Socket(host, port);
      BufferedReader reader = null;
      try {
          OutputStream outstream = sock.getOutputStream();
          outstream.write(cmd.getBytes("US-ASCII"));
          outstream.flush();
          // this replicates NC - close the output stream before reading
          sock.shutdownOutput();

          reader =
              new BufferedReader(
                      new InputStreamReader(sock.getInputStream()));
          StringBuilder sb = new StringBuilder();
          String line;
          while((line = reader.readLine()) != null) {
              sb.append(line + "\n");
          }
          return sb.toString();
      } finally {
          sock.close();
          if (reader != null) {
              reader.close();
          }
      }
  }
  
  public static List<HostPort> parseHostPortList(String hplist) {
    ArrayList<HostPort> alist = new ArrayList<HostPort>();
    for (String hp : hplist.split(",")) {
      int idx = hp.lastIndexOf(':');
      String host = hp.substring(0, idx);
      int port;
      try {
        port = Integer.parseInt(hp.substring(idx + 1));
      } catch (RuntimeException e) {
        throw new RuntimeException("Problem parsing " + hp + e.toString());
      }
      alist.add(new HostPort(host, port));
    }
    return alist;
  }
}
