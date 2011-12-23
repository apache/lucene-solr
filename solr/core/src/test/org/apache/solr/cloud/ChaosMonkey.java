package org.apache.solr.cloud;

/**
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

import java.net.BindException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.FullSolrCloudTest.CloudJettyRunner;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.zookeeper.KeeperException;
import org.mortbay.jetty.servlet.FilterHolder;

/**
 * The monkey can stop random or specific jetties used with SolrCloud.
 * 
 * It can also run in a background thread and start and stop jetties
 * randomly.
 *
 */
public class ChaosMonkey {

  private Map<String,List<CloudJettyRunner>> shardToJetty;
  private ZkTestServer zkServer;
  private ZkStateReader zkStateReader;
  private String collection;
  private Random random;
  private volatile boolean stop = false;
  private AtomicInteger stops = new AtomicInteger();
  private AtomicInteger starts = new AtomicInteger();
  
  public ChaosMonkey(ZkTestServer zkServer, ZkStateReader zkStateReader,
      String collection, Map<String,List<CloudJettyRunner>> shardToJetty,
      Random random) {
    this.shardToJetty = shardToJetty;
    this.zkServer = zkServer;
    this.zkStateReader = zkStateReader;
    this.collection = collection;
    this.random = random;
  }
  
  public void expireSession(CloudJettyRunner cloudJetty) {
    SolrDispatchFilter solrDispatchFilter = (SolrDispatchFilter) cloudJetty.jetty.getDispatchFilter().getFilter();
    long sessionId = solrDispatchFilter.getCores().getZkController().getZkClient().getSolrZooKeeper().getSessionId();
    zkServer.expire(sessionId);
  }
  
  public JettySolrRunner stopShard(String slice, int index) throws Exception {
    JettySolrRunner jetty = shardToJetty.get(slice).get(index).jetty;
    stopJetty(jetty);
    return jetty;
  }

  public void stopJetty(JettySolrRunner jetty) throws Exception {
    if (jetty.isRunning()) {
      stops.incrementAndGet();
    }
    
    stop(jetty);
  }

  public void killJetty(JettySolrRunner jetty) throws Exception {
    if (jetty.isRunning()) {
      stops.incrementAndGet();
    }
    
    kill(jetty);

  }
  
  public static void stop(JettySolrRunner jetty) throws Exception {
    // get a clean shutdown so that no dirs are left open...
    FilterHolder fh = jetty.getDispatchFilter();
    if (fh != null) {
      SolrDispatchFilter sdf = (SolrDispatchFilter) fh.getFilter();
      if (sdf != null) {
        sdf.destroy();
      }
    }

    jetty.stop();
  }
  
  public static void kill(JettySolrRunner jetty) throws Exception {
    jetty.stop();
    
    FilterHolder fh = jetty.getDispatchFilter();
    if (fh != null) {
      SolrDispatchFilter sdf = (SolrDispatchFilter) fh.getFilter();
      if (sdf != null) {
        sdf.destroy();
      }
    }
  }
  
  public void stopShard(String slice) throws Exception {
    List<CloudJettyRunner> jetties = shardToJetty.get(slice);
    for (CloudJettyRunner jetty : jetties) {
      stopJetty(jetty.jetty);
    }
  }
  
  public void stopShardExcept(String slice, String shardName) throws Exception {
    List<CloudJettyRunner> jetties = shardToJetty.get(slice);
    for (CloudJettyRunner jetty : jetties) {
      if (!jetty.nodeName.equals(shardName)) {
        stopJetty(jetty.jetty);
      }
    }
  }
  
  public JettySolrRunner getShard(String slice, int index) throws Exception {
    JettySolrRunner jetty = shardToJetty.get(slice).get(index).jetty;
    return jetty;
  }
  
  public JettySolrRunner stopRandomShard() throws Exception {
    // add all the shards to a list
    Map<String,Slice> slices = zkStateReader.getCloudState().getSlices(collection);
    
    List<String> sliceKeyList = new ArrayList<String>(slices.size());
    sliceKeyList.addAll(slices.keySet());
    String sliceName = sliceKeyList.get(random.nextInt(sliceKeyList.size()));
    
    return stopRandomShard(sliceName);
  }
  
  public JettySolrRunner stopRandomShard(String slice) throws Exception {
    JettySolrRunner jetty = getRandomSacraficialShard(slice);
    if (jetty != null) {
      stopJetty(jetty);
    }
    return jetty;
  }
  
  
  public JettySolrRunner killRandomShard() throws Exception {
    // add all the shards to a list
    Map<String,Slice> slices = zkStateReader.getCloudState().getSlices(collection);
    
    List<String> sliceKeyList = new ArrayList<String>(slices.size());
    sliceKeyList.addAll(slices.keySet());
    String sliceName = sliceKeyList.get(random.nextInt(sliceKeyList.size()));
    
    return killRandomShard(sliceName);
  }
  
  public JettySolrRunner killRandomShard(String slice) throws Exception {
    JettySolrRunner jetty = getRandomSacraficialShard(slice);
    if (jetty != null) {
      killJetty(jetty);
    }
    return jetty;
  }
  
  public JettySolrRunner getRandomSacraficialShard(String slice) throws KeeperException, InterruptedException {
    // get latest cloud state
    zkStateReader.updateCloudState(true);
    Slice theShards = zkStateReader.getCloudState().getSlices(collection)
        .get(slice);
    int numRunning = 0;
    int numRecovering = 0;
    int numActive = 0;
    
    for (CloudJettyRunner cloudJetty : shardToJetty.get(slice)) {
      boolean running = true;
      
      ZkNodeProps props = theShards.getShards().get(cloudJetty.shardName);
      if (props == null) {
        throw new RuntimeException("shard name " + cloudJetty.shardName + " not found in " + theShards.getShards().keySet());
      }
      String state = props.get(ZkStateReader.STATE_PROP);
      String nodeName = props.get(ZkStateReader.NODE_NAME_PROP);
      
      if (!cloudJetty.jetty.isRunning()
          || !state.equals(ZkStateReader.ACTIVE)
          || !zkStateReader.getCloudState().liveNodesContain(nodeName)) {
        running = false;
      }
      
      if (cloudJetty.jetty.isRunning()
          && state.equals(ZkStateReader.RECOVERING)
          && zkStateReader.getCloudState().liveNodesContain(nodeName)) {
        numRecovering++;
      }
      
      if (cloudJetty.jetty.isRunning()
          && state.equals(ZkStateReader.ACTIVE)
          && zkStateReader.getCloudState().liveNodesContain(nodeName)) {
        numActive++;
      }
      
      if (running) {
        numRunning++;
      }
    }
    
    if (numActive < 2) {
      // we cannot kill anyone
      return null;
    }
    
    // get random shard
    List<CloudJettyRunner> jetties = shardToJetty.get(slice);
    int index = random.nextInt(jetties.size() - 1);
    JettySolrRunner jetty = jetties.get(index).jetty;
    System.out.println("sac shard "+ jetty.getLocalPort());
    
    return jetty;
  }
  
  // synchronously starts and stops shards randomly, unless there is only one
  // active shard up for a slice or if there is one active and others recovering
  public void startTheMonkey() {
    stop = false;
    new Thread() {
      private List<JettySolrRunner> deadPool = new ArrayList<JettySolrRunner>();

      @Override
      public void run() {
        while (!stop) {
          try {
            Thread.sleep(500);
            
            if (random.nextBoolean()) {
             if (!deadPool.isEmpty()) {
               System.out.println("start jetty");
               JettySolrRunner jetty = deadPool.remove(random.nextInt(deadPool.size()));
               try {
                 jetty.start();
               } catch (BindException e) {
                 jetty.stop();
                 sleep(2000);
                 jetty.start();
               }
               System.out.println("started on port:" + jetty.getLocalPort());
               starts.incrementAndGet();
               continue;
             }
            }
            
            JettySolrRunner jetty;
            if (random.nextBoolean()) {
              jetty = stopRandomShard();
            } else {
              jetty = killRandomShard();
            }
            if (jetty == null) {
              System.out.println("we cannot kill");
            } else {
              deadPool.add(jetty);
            }
          } catch (InterruptedException e) {
            //
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        
        System.out.println("I stopped " + stops + " and I started " + starts);
      }
    }.start();
  }
  
  public void stopTheMonkey() {
    stop = true;
  }

  public int getStarts() {
    return starts.get();
  }

}