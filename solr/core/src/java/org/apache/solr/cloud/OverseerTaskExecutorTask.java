package org.apache.solr.cloud;

import org.apache.solr.client.solrj.cloud.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.NodeMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.ReplicaMutator;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OverseerTaskExecutorTask implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ZkController zkController;
  private final SolrCloudManager cloudManager;
  private final SolrZkClient zkClient;
  private final Overseer overseer;
  private final ZkStateWriter zkStateWriter;
  private final ZkNodeProps message;

  public OverseerTaskExecutorTask(CoreContainer cc, ZkStateWriter zkStateWriter, ZkNodeProps message) {
    this.zkController = cc.getZkController();
    this.zkClient = zkController.getZkClient();
    this.cloudManager = zkController.getSolrCloudManager();
    this.overseer = zkController.getOverseer();
    this.zkStateWriter = zkStateWriter;
    this.message = message;
  }


  private void processQueueItem(ZkNodeProps message) throws Exception {
    if (log.isDebugEnabled()) log.debug("Consume state update from queue {} {}", message);

    // assert clusterState != null;

    //  if (clusterState.getZNodeVersion() == 0 || clusterState.getZNodeVersion() > lastVersion) {

    final String operation = message.getStr(Overseer.QUEUE_OPERATION);
    if (operation == null) {
      log.error("Message missing " + Overseer.QUEUE_OPERATION + ":" + message);
      return;
    }

    log.info("Queue operation is {}", operation);

    ClusterState cs = zkStateWriter.getClusterstate(true);

    log.info("Process message {} {}", message, operation);
    ClusterState newClusterState = processMessage(message, operation, cs);

    log.info("Enqueue message {}", operation);
    zkStateWriter.enqueueUpdate(newClusterState, true);


    if (log.isDebugEnabled()) log.debug("State update consumed from queue {}", message);
  }

  private ClusterState processMessage(final ZkNodeProps message, final String operation, ClusterState clusterState) {
    if (log.isDebugEnabled()) {
      log.debug("processMessage(ZkNodeProps message={}, String operation={} clusterState={})", message, operation, clusterState);
    }

    OverseerAction overseerAction = OverseerAction.get(operation);
    if (overseerAction == null) {
      throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
    }
    switch (overseerAction) {
      case STATE:
        return new ReplicaMutator(cloudManager).setState(clusterState, message);
      case LEADER:
        return new SliceMutator(cloudManager).setShardLeader(clusterState, message);
      case ADDROUTINGRULE:
        return new SliceMutator(cloudManager).addRoutingRule(clusterState, message);
      case REMOVEROUTINGRULE:
        return new SliceMutator(cloudManager).removeRoutingRule(clusterState, message);
      case UPDATESHARDSTATE:
        return new SliceMutator(cloudManager).updateShardState(clusterState, message);
      //          case QUIT:
      //            if (myId.equals(message.get(ID))) {
      //              log.info("Quit command received {} {}", message, LeaderElector.getNodeName(myId));
      //              try {
      //                overseerCollectionConfigSetProcessor.close();
      //              } catch (IOException e) {
      //                log.error("IOException", e);
      //              }
      //              close();
      //            } else {
      //              log.warn("Overseer received wrong QUIT message {}", message);
      //            }
      //            break;
      case DOWNNODE:
        return new NodeMutator().downNode(clusterState, message);
      default:
        throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());

    }
  }

  @Override
  public void run() {
    log.info("OverseerTaskExecutorTask, going to process message {}", message);

    try {
      processQueueItem(message);
    } catch (Exception e) {
      log.error("Failed to process message " + message, e);
    }
  }

  public static class WriteTask implements Runnable {
    CoreContainer coreContainer;
    ZkStateWriter zkStateWriter;

    public WriteTask(CoreContainer coreContainer, ZkStateWriter zkStateWriter) {
      this.coreContainer = coreContainer;
      this.zkStateWriter = zkStateWriter;
    }

    @Override
    public void run() {
      try {
        zkStateWriter.writePendingUpdates();
      } catch (NullPointerException e) {
        if (log.isDebugEnabled()) log.debug("Won't write pending updates, zkStateWriter=null");
      } catch (Exception e) {
        log.error("Failed to process pending updates", e);
      }
    }
  }
}
