package org.apache.solr.cloud;

import java.net.ConnectException;
import java.net.SocketException;
import java.util.List;

import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestRecovery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Background daemon thread that tries to send the REQUESTRECOVERY to a downed
 * replica; used by a shard leader to nag a replica into recovering after the
 * leader experiences an error trying to send an update request to the replica.
 */
public class LeaderInitiatedRecoveryThread extends Thread {

  public final static Logger log = LoggerFactory.getLogger(LeaderInitiatedRecoveryThread.class);

  protected ZkController zkController;
  protected CoreContainer coreContainer;
  protected String collection;
  protected String shardId;
  protected ZkCoreNodeProps nodeProps;
  protected int maxTries;
  protected String leaderCoreNodeName;
  
  public LeaderInitiatedRecoveryThread(ZkController zkController, 
                                       CoreContainer cc, 
                                       String collection, 
                                       String shardId, 
                                       ZkCoreNodeProps nodeProps,
                                       int maxTries,
                                       String leaderCoreNodeName)
  {
    super("LeaderInitiatedRecoveryThread-"+nodeProps.getCoreName());
    this.zkController = zkController;
    this.coreContainer = cc;
    this.collection = collection;
    this.shardId = shardId;    
    this.nodeProps = nodeProps;
    this.maxTries = maxTries;
    this.leaderCoreNodeName = leaderCoreNodeName;
    
    setDaemon(true);
  }
  
  public void run() {
    long startMs = System.currentTimeMillis();
    try {
      sendRecoveryCommandWithRetry();
    } catch (Exception exc) {
      log.error(getName()+" failed due to: "+exc, exc);
      if (exc instanceof SolrException) {
        throw (SolrException)exc;
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, exc);
      }
    }
    long diffMs = (System.currentTimeMillis() - startMs);
    log.info(getName()+" completed successfully after running for "+Math.round(diffMs/1000L)+" secs");    
  }
  
  protected void sendRecoveryCommandWithRetry() throws Exception {    
    int tries = 0;
    long waitBetweenTriesMs = 5000L;
    boolean continueTrying = true;
        
    String recoveryUrl = nodeProps.getBaseUrl();
    String replicaNodeName = nodeProps.getNodeName();
    String coreNeedingRecovery = nodeProps.getCoreName();
    String replicaCoreNodeName = ((Replica) nodeProps.getNodeProps()).getName();
    String replicaUrl = nodeProps.getCoreUrl();
    
    log.info(getName()+" started running to send REQUESTRECOVERY command to "+replicaUrl+
        "; will try for a max of "+(maxTries * (waitBetweenTriesMs/1000))+" secs");

    RequestRecovery recoverRequestCmd = new RequestRecovery();
    recoverRequestCmd.setAction(CoreAdminAction.REQUESTRECOVERY);
    recoverRequestCmd.setCoreName(coreNeedingRecovery);
    
    while (continueTrying && ++tries <= maxTries) {
      if (tries > 1) {
        log.warn("Asking core={} coreNodeName={} on " + recoveryUrl +
            " to recover; unsuccessful after "+tries+" of "+maxTries+" attempts so far ...", coreNeedingRecovery, replicaCoreNodeName);
      } else {
        log.info("Asking core={} coreNodeName={} on " + recoveryUrl + " to recover", coreNeedingRecovery, replicaCoreNodeName);
      }
      
      HttpSolrServer server = new HttpSolrServer(recoveryUrl);
      try {
        server.setSoTimeout(60000);
        server.setConnectionTimeout(15000);
        try {
          server.request(recoverRequestCmd);
          
          log.info("Successfully sent " + CoreAdminAction.REQUESTRECOVERY +
              " command to core={} coreNodeName={} on " + recoveryUrl, coreNeedingRecovery, replicaCoreNodeName);
          
          continueTrying = false; // succeeded, so stop looping
        } catch (Throwable t) {
          Throwable rootCause = SolrException.getRootCause(t);
          boolean wasCommError =
              (rootCause instanceof ConnectException ||
                  rootCause instanceof ConnectTimeoutException ||
                  rootCause instanceof NoHttpResponseException ||
                  rootCause instanceof SocketException);

          SolrException.log(log, recoveryUrl + ": Could not tell a replica to recover", t);
          
          if (!wasCommError) {
            continueTrying = false;
          }                                                
        }
      } finally {
        server.shutdown();
      }
      
      // wait a few seconds
      if (continueTrying) {
        try {
          Thread.sleep(waitBetweenTriesMs);
        } catch (InterruptedException ignoreMe) {
          Thread.currentThread().interrupt();          
        }
        
        if (coreContainer.isShutDown()) {
          log.warn("Stop trying to send recovery command to downed replica core={} coreNodeName={} on "
              + replicaNodeName + " because my core container is closed.", coreNeedingRecovery, replicaCoreNodeName);
          continueTrying = false;
          break;
        }
        
        // see if the replica's node is still live, if not, no need to keep doing this loop
        ZkStateReader zkStateReader = zkController.getZkStateReader();
        try {
          zkStateReader.updateClusterState(true);
        } catch (Exception exc) {
          log.warn("Error when updating cluster state: "+exc);
        }        
        
        if (!zkStateReader.getClusterState().liveNodesContain(replicaNodeName)) {
          log.warn("Node "+replicaNodeName+" hosting core "+coreNeedingRecovery+
              " is no longer live. No need to keep trying to tell it to recover!");
          continueTrying = false;
          break;
        }

        // stop trying if I'm no longer the leader
        if (leaderCoreNodeName != null && collection != null) {
          String leaderCoreNodeNameFromZk = null;
          try {
            leaderCoreNodeNameFromZk = zkController.getZkStateReader().getLeaderRetry(collection, shardId, 1000).getName();
          } catch (Exception exc) {
            log.error("Failed to determine if " + leaderCoreNodeName + " is still the leader for " + collection +
                " " + shardId + " before starting leader-initiated recovery thread for " + replicaUrl + " due to: " + exc);
          }
          if (!leaderCoreNodeName.equals(leaderCoreNodeNameFromZk)) {
            log.warn("Stop trying to send recovery command to downed replica core=" + coreNeedingRecovery +
                ",coreNodeName=" + replicaCoreNodeName + " on " + replicaNodeName + " because " +
                leaderCoreNodeName + " is no longer the leader! New leader is " + leaderCoreNodeNameFromZk);
            continueTrying = false;
            break;
          }
        }

        // additional safeguard against the replica trying to be in the active state
        // before acknowledging the leader initiated recovery command
        if (continueTrying && collection != null && shardId != null) {
          try {
            // call out to ZooKeeper to get the leader-initiated recovery state
            String lirState = 
                zkController.getLeaderInitiatedRecoveryState(collection, shardId, replicaCoreNodeName);
            
            if (lirState == null) {
              log.warn("Stop trying to send recovery command to downed replica core="+coreNeedingRecovery+
                  ",coreNodeName=" + replicaCoreNodeName + " on "+replicaNodeName+" because the znode no longer exists.");
              continueTrying = false;
              break;              
            }
            
            if (ZkStateReader.RECOVERING.equals(lirState)) {
              // replica has ack'd leader initiated recovery and entered the recovering state
              // so we don't need to keep looping to send the command
              continueTrying = false;  
              log.info("Replica "+coreNeedingRecovery+
                  " on node "+replicaNodeName+" ack'd the leader initiated recovery state, "
                      + "no need to keep trying to send recovery command");
            } else {
              String leaderCoreNodeName = zkStateReader.getLeaderRetry(collection, shardId, 5000).getName();
              List<ZkCoreNodeProps> replicaProps = 
                  zkStateReader.getReplicaProps(collection, shardId, leaderCoreNodeName);
              if (replicaProps != null && replicaProps.size() > 0) {
                String replicaState = replicaProps.get(0).getState();
                if (ZkStateReader.ACTIVE.equals(replicaState)) {
                  // replica published its state as "active", 
                  // which is bad if lirState is still "down"
                  if (ZkStateReader.DOWN.equals(lirState)) {
                    // OK, so the replica thinks it is active, but it never ack'd the leader initiated recovery
                    // so its state cannot be trusted and it needs to be told to recover again ... and we keep looping here
                    log.warn("Replica core={} coreNodeName={} set to active but the leader thinks it should be in recovery;"
                        + " forcing it back to down state to re-run the leader-initiated recovery process; props: "+replicaProps.get(0), coreNeedingRecovery, replicaCoreNodeName);
                    zkController.ensureReplicaInLeaderInitiatedRecovery(collection, 
                        shardId, replicaUrl, nodeProps, true); // force republish state to "down"
                  }
                }                    
              }                    
            }                  
          } catch (Exception ignoreMe) {
            log.warn("Failed to determine state of core={} coreNodeName={} due to: "+ignoreMe, coreNeedingRecovery, replicaCoreNodeName);
            // eventually this loop will exhaust max tries and stop so we can just log this for now
          }                
        }
      }
    }
    
    // replica is no longer in recovery on this node (may be handled on another node)
    zkController.removeReplicaFromLeaderInitiatedRecoveryHandling(replicaUrl);
    
    if (continueTrying) {
      // ugh! this means the loop timed out before the recovery command could be delivered
      // how exotic do we want to get here?
      log.error("Timed out after waiting for "+(tries * (waitBetweenTriesMs/1000))+
          " secs to send the recovery request to: "+replicaUrl+"; not much more we can do here?");
      
      // TODO: need to raise a JMX event to allow monitoring tools to take over from here
      
    }    
  }
}
