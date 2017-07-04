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
package org.apache.solr.cloud;

import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestRecovery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.zookeeper.KeeperException;
import org.apache.solr.util.RTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.List;

/**
 * Background daemon thread that tries to send the REQUESTRECOVERY to a downed
 * replica; used by a shard leader to nag a replica into recovering after the
 * leader experiences an error trying to send an update request to the replica.
 */
public class LeaderInitiatedRecoveryThread extends Thread {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected ZkController zkController;
  protected CoreContainer coreContainer;
  protected String collection;
  protected String shardId;
  protected ZkCoreNodeProps nodeProps;
  protected int maxTries;
  private CoreDescriptor leaderCd;
  
  public LeaderInitiatedRecoveryThread(ZkController zkController, 
                                       CoreContainer cc, 
                                       String collection, 
                                       String shardId, 
                                       ZkCoreNodeProps nodeProps,
                                       int maxTries,
                                       CoreDescriptor leaderCd)
  {
    super("LeaderInitiatedRecoveryThread-"+nodeProps.getCoreName());
    this.zkController = zkController;
    this.coreContainer = cc;
    this.collection = collection;
    this.shardId = shardId;    
    this.nodeProps = nodeProps;
    this.maxTries = maxTries;
    this.leaderCd = leaderCd;
    setDaemon(true);
  }
  
  public void run() {
    RTimer timer = new RTimer();

    String replicaCoreName = nodeProps.getCoreName();
    String replicaCoreNodeName = ((Replica) nodeProps.getNodeProps()).getName();
    String replicaNodeName = nodeProps.getNodeName();
    final String replicaUrl = nodeProps.getCoreUrl();

    if (!zkController.isReplicaInRecoveryHandling(replicaUrl)) {
      throw new SolrException(ErrorCode.INVALID_STATE, "Replica: " + replicaUrl + " should have been marked under leader initiated recovery in ZkController but wasn't.");
    }

    boolean sendRecoveryCommand = publishDownState(replicaCoreName, replicaCoreNodeName, replicaNodeName, replicaUrl, false);

    if (sendRecoveryCommand)  {
      try {
        sendRecoveryCommandWithRetry();
      } catch (Exception exc) {
        log.error(getName()+" failed due to: "+exc, exc);
        if (exc instanceof SolrException) {
          throw (SolrException)exc;
        } else {
          throw new SolrException(ErrorCode.SERVER_ERROR, exc);
        }
      } finally {
        zkController.removeReplicaFromLeaderInitiatedRecoveryHandling(replicaUrl);
      }
    } else  {
      // replica is no longer in recovery on this node (may be handled on another node)
      zkController.removeReplicaFromLeaderInitiatedRecoveryHandling(replicaUrl);
    }
    log.info("{} completed successfully after running for {}ms", getName(), timer.getTime());
  }

  public boolean publishDownState(String replicaCoreName, String replicaCoreNodeName, String replicaNodeName, String replicaUrl, boolean forcePublishState) {
    boolean sendRecoveryCommand = true;
    boolean publishDownState = false;

    if (zkController.getZkStateReader().getClusterState().liveNodesContain(replicaNodeName)) {
      try {
        // create a znode that requires the replica needs to "ack" to verify it knows it was out-of-sync
        updateLIRState(replicaCoreNodeName);

        log.info("Put replica core={} coreNodeName={} on " +
            replicaNodeName + " into leader-initiated recovery.", replicaCoreName, replicaCoreNodeName);
        publishDownState = true;
      } catch (Exception e) {
        Throwable setLirZnodeFailedCause = SolrException.getRootCause(e);
        log.error("Leader failed to set replica " +
            nodeProps.getCoreUrl() + " state to DOWN due to: " + setLirZnodeFailedCause, setLirZnodeFailedCause);
        if (setLirZnodeFailedCause instanceof KeeperException.SessionExpiredException
            || setLirZnodeFailedCause instanceof KeeperException.ConnectionLossException
            || setLirZnodeFailedCause instanceof ZkController.NotLeaderException) {
          // our session is expired, which means our state is suspect, so don't go
          // putting other replicas in recovery (see SOLR-6511)
          sendRecoveryCommand = false;
          forcePublishState = false; // no need to force publish any state in this case
        } // else will go ahead and try to send the recovery command once after this error
      }
    } else  {
      log.info("Node " + replicaNodeName +
              " is not live, so skipping leader-initiated recovery for replica: core={} coreNodeName={}",
          replicaCoreName, replicaCoreNodeName);
      // publishDownState will be false to avoid publishing the "down" state too many times
      // as many errors can occur together and will each call into this method (SOLR-6189)
      forcePublishState = false; // no need to force publish the state because replica is not live
      sendRecoveryCommand = false; // no need to send recovery messages as well
    }

    try {
      if (publishDownState || forcePublishState) {
        ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, "state",
            ZkStateReader.STATE_PROP, Replica.State.DOWN.toString(),
            ZkStateReader.BASE_URL_PROP, nodeProps.getBaseUrl(),
            ZkStateReader.CORE_NAME_PROP, nodeProps.getCoreName(),
            ZkStateReader.NODE_NAME_PROP, nodeProps.getNodeName(),
            ZkStateReader.SHARD_ID_PROP, shardId,
            ZkStateReader.COLLECTION_PROP, collection);
        log.warn("Leader is publishing core={} coreNodeName ={} state={} on behalf of un-reachable replica {}",
            replicaCoreName, replicaCoreNodeName, Replica.State.DOWN.toString(), replicaUrl);
        zkController.getOverseerJobQueue().offer(Utils.toJSON(m));
      }
    } catch (Exception e) {
      log.error("Could not publish 'down' state for replicaUrl: {}", replicaUrl, e);
    }

    return sendRecoveryCommand;
  }

  /*
  protected scope for testing purposes
   */
  protected void updateLIRState(String replicaCoreNodeName) {
    zkController.updateLeaderInitiatedRecoveryState(collection,
        shardId,
        replicaCoreNodeName, Replica.State.DOWN, leaderCd, true);
  }

  protected void sendRecoveryCommandWithRetry() throws Exception {    
    int tries = 0;
    long waitBetweenTriesMs = 5000L;
    boolean continueTrying = true;

    String replicaCoreName = nodeProps.getCoreName();
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

      try (HttpSolrClient client = new HttpSolrClient.Builder(recoveryUrl)
          .withConnectionTimeout(15000)
          .withSocketTimeout(60000)
          .build()) {
        try {
          client.request(recoverRequestCmd);
          
          log.info("Successfully sent " + CoreAdminAction.REQUESTRECOVERY +
              " command to core={} coreNodeName={} on " + recoveryUrl, coreNeedingRecovery, replicaCoreNodeName);
          
          continueTrying = false; // succeeded, so stop looping
        } catch (Exception t) {
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
        if (!zkStateReader.getClusterState().liveNodesContain(replicaNodeName)) {
          log.warn("Node "+replicaNodeName+" hosting core "+coreNeedingRecovery+
              " is no longer live. No need to keep trying to tell it to recover!");
          continueTrying = false;
          break;
        }

        String leaderCoreNodeName = leaderCd.getCloudDescriptor().getCoreNodeName();
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
          if (!leaderCd.getCloudDescriptor().isLeader()) {
            log.warn("Stop trying to send recovery command to downed replica core=" + coreNeedingRecovery +
                ",coreNodeName=" + replicaCoreNodeName + " on " + replicaNodeName + " because " +
                leaderCoreNodeName + " is no longer the leader!");
            continueTrying = false;
            break;
          }
        }

        // additional safeguard against the replica trying to be in the active state
        // before acknowledging the leader initiated recovery command
        if (collection != null && shardId != null) {
          try {
            // call out to ZooKeeper to get the leader-initiated recovery state
            final Replica.State lirState = zkController.getLeaderInitiatedRecoveryState(collection, shardId, replicaCoreNodeName);
            
            if (lirState == null) {
              log.warn("Stop trying to send recovery command to downed replica core="+coreNeedingRecovery+
                  ",coreNodeName=" + replicaCoreNodeName + " on "+replicaNodeName+" because the znode no longer exists.");
              continueTrying = false;
              break;              
            }
            
            if (lirState == Replica.State.RECOVERING) {
              // replica has ack'd leader initiated recovery and entered the recovering state
              // so we don't need to keep looping to send the command
              continueTrying = false;  
              log.info("Replica "+coreNeedingRecovery+
                  " on node "+replicaNodeName+" ack'd the leader initiated recovery state, "
                      + "no need to keep trying to send recovery command");
            } else {
              String lcnn = zkStateReader.getLeaderRetry(collection, shardId, 5000).getName();
              List<ZkCoreNodeProps> replicaProps = 
                  zkStateReader.getReplicaProps(collection, shardId, lcnn);
              if (replicaProps != null && replicaProps.size() > 0) {
                for (ZkCoreNodeProps prop : replicaProps) {
                  final Replica replica = (Replica) prop.getNodeProps();
                  if (replicaCoreNodeName.equals(replica.getName())) {
                    if (replica.getState() == Replica.State.ACTIVE) {
                      // replica published its state as "active",
                      // which is bad if lirState is still "down"
                      if (lirState == Replica.State.DOWN) {
                        // OK, so the replica thinks it is active, but it never ack'd the leader initiated recovery
                        // so its state cannot be trusted and it needs to be told to recover again ... and we keep looping here
                        log.warn("Replica core={} coreNodeName={} set to active but the leader thinks it should be in recovery;"
                            + " forcing it back to down state to re-run the leader-initiated recovery process; props: " + replicaProps.get(0), coreNeedingRecovery, replicaCoreNodeName);
                        publishDownState(replicaCoreName, replicaCoreNodeName, replicaNodeName, replicaUrl, true);
                      }
                    }
                    break;
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
