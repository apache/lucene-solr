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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.cloud.ZkController.ContextKey;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.SolrZooKeeper;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.IOUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader Election process. This class contains the logic by which a
 * leader is chosen. First call * {@link #setup(ElectionContext)} to ensure
 * the election process is init'd. Next call
 * {@link #joinElection(ElectionContext, boolean)} to start the leader election.
 *
 * The implementation follows the classic ZooKeeper recipe of creating an
 * ephemeral, sequential node for each candidate and then looking at the set
 * of such nodes - if the created node is the lowest sequential node, the
 * candidate that created the node is the leader. If not, the candidate puts
 * a watch on the next lowest node it finds, and if that node goes down,
 * starts the whole process over by checking if it's the lowest sequential node, etc.
 *
 */
public class LeaderElector implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ELECTION_NODE = "/election";

  public final static Pattern LEADER_SEQ = Pattern.compile(".*?/?.*?-n_(\\d+)");
  private final static Pattern SESSION_ID = Pattern.compile(".*?/?(.*?-.*?)-n_\\d+");

  protected final SolrZkClient zkClient;
  private final ZkController zkController;

  private volatile ElectionContext context;

  private volatile ElectionWatcher watcher;

  private final Map<ContextKey,ElectionContext> electionContexts;
  private final ContextKey contextKey;
  private volatile boolean isClosed;

  //  public LeaderElector(SolrZkClient zkClient) {
//    this.zkClient = zkClient;
//    this.contextKey = null;
//    this.electionContexts = new ConcurrentHashMap<>(132, 0.75f, 50);
//  }

  public LeaderElector(ZkController zkController, ContextKey key, Map<ContextKey,ElectionContext> electionContexts) {

    this.zkClient = zkController.getZkClient();
    this.zkController = zkController;
    this.electionContexts = electionContexts;
    this.contextKey = key;
  }

  public ElectionContext getContext() {
    return context;
  }

  /**
   * Check if the candidate with the given n_* sequence number is the leader.
   * If it is, set the leaderId on the leader zk node. If it is not, start
   * watching the candidate that is in line before this one - if it goes down, check
   * if this candidate is the leader again.
   *
   * @param replacement has someone else been the leader already?
   */
  private boolean checkIfIamLeader(final ElectionContext context, boolean replacement) throws KeeperException,
          InterruptedException, IOException {
    if (isClosed || (zkController != null && zkController.getCoreContainer().isShutDown())) {
      if (log.isDebugEnabled()) log.debug("Will not checkIfIamLeader, elector is closed");
      return false;
    }

    log.info("Check if I am leader {}", context.getClass().getSimpleName());

    ParWork.getRootSharedExecutor().submit(() -> {
      context.checkIfIamLeaderFired();
    });

    boolean checkAgain = false;

    // get all other numbers...
    final String holdElectionPath = context.electionPath + ELECTION_NODE;
    List<String> seqs = zkClient.getChildren(holdElectionPath, null, true);
    sortSeqs(seqs);

    String leaderSeqNodeName;
    try {
      leaderSeqNodeName = context.leaderSeqPath.substring(context.leaderSeqPath.lastIndexOf('/') + 1);
    } catch (NullPointerException e) {
      if (log.isDebugEnabled()) log.debug("leaderSeqPath has been removed, bailing");
      return false;
    }
    if (!seqs.contains(leaderSeqNodeName)) {
      log.warn("Our node is no longer in line to be leader");
      return false;
    }

    if (leaderSeqNodeName.equals(seqs.get(0))) {
      // I am the leader
      log.info("I am the potential leader {}, running leader process", context.leaderProps);
      ParWork.getRootSharedExecutor().submit(() -> {
        try {
          if (isClosed || (zkController != null && zkController.getCoreContainer().isShutDown())) {
            if (log.isDebugEnabled()) log.debug("Elector is closed, will not try and run leader processes");
            return;
          }
          runIamLeaderProcess(context, replacement);
        } catch (Exception e) {
          log.error("", e);
        }
      });

    } else {
      log.info("I am not the leader - watch the node below me {}", context.getClass().getSimpleName());
      String toWatch = seqs.get(0);
      for (String node : seqs) {
        if (leaderSeqNodeName.equals(node)) {
          break;
        }
        toWatch = node;
      }
      try {
        String watchedNode = holdElectionPath + "/" + toWatch;

        ElectionWatcher oldWatcher = watcher;
        if (oldWatcher != null) {
          oldWatcher.close();
        }
        zkClient.exists(watchedNode, watcher = new ElectionWatcher(context.leaderSeqPath,
            watchedNode, getSeq(context.leaderSeqPath), context));
        if (log.isDebugEnabled()) log.debug("Watching path {} to know if I could be the leader", watchedNode);
      } catch (KeeperException.SessionExpiredException e) {
        log.error("ZooKeeper session has expired");
        throw e;
      } catch (KeeperException.NoNodeException e) {
        log.info("the previous node disappeared, check if we are the leader again");
        checkAgain = true;
      } catch (KeeperException e) {
        // we couldn't set our watch for some other reason, retry
        log.info("Failed setting election watch, retrying {} {}", e.getClass().getName(), e.getMessage());
        checkAgain = true;
      } catch (Exception e) {
        // we couldn't set our watch for some other reason, retry
        log.info("Failed setting election watch {} {}", e.getClass().getName(), e.getMessage());
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }

      if (checkAgain) {
        return true;
      }
    }
    return false;
  }

  // TODO: get this core param out of here
  protected void runIamLeaderProcess(final ElectionContext context, boolean weAreReplacement) throws KeeperException,
          InterruptedException, IOException {
    context.runLeaderProcess(context, weAreReplacement,0);
  }

  /**
   * Returns int given String of form n_0000000001 or n_0000000003, etc.
   *
   * @return sequence number
   */
  public static int getSeq(String nStringSequence) {
    int seq = 0;
    Matcher m = LEADER_SEQ.matcher(nStringSequence);
    if (m.matches()) {
      seq = Integer.parseInt(m.group(1));
    } else {
      throw new IllegalStateException("Could not find regex match in:"
              + nStringSequence);
    }
    return seq;
  }

  private String getNodeId(String nStringSequence) {
    String id;
    Matcher m = SESSION_ID.matcher(nStringSequence);
    if (m.matches()) {
      id = m.group(1);
    } else {
      throw new IllegalStateException("Could not find regex match in:"
              + nStringSequence);
    }
    return id;
  }

  public static String getNodeName(String nStringSequence){

    return nStringSequence;

  }

  public boolean joinElection(ElectionContext context, boolean replacement) throws KeeperException, InterruptedException, IOException {
    return joinElection(context,replacement, false);
  }

  /**
   * Begin participating in the election process. Gets a new sequential number
   * and begins watching the node with the sequence number before it, unless it
   * is the lowest number, in which case, initiates the leader process. If the
   * node that is watched goes down, check if we are the new lowest node, else
   * watch the next lowest numbered node.
   *
   * @return sequential node number
   */
  public boolean joinElection(ElectionContext context, boolean replacement,boolean joinAtHead) throws KeeperException, InterruptedException, IOException {
    if (isClosed || (zkController != null && zkController.getCoreContainer().isShutDown())) {
      if (log.isDebugEnabled()) log.debug("Will not join election, elector is closed");
      return false;
    }

    ParWork.getRootSharedExecutor().submit(() -> {
      context.joinedElectionFired();
    });

    final String shardsElectZkPath = context.electionPath + LeaderElector.ELECTION_NODE;

    long sessionId = zkClient.getSolrZooKeeper().getSessionId();
    String id = sessionId + "-" + context.id;
    String leaderSeqPath = null;
    boolean cont = true;
    int tries = 0;
    while (cont) {
      try {
        if (joinAtHead){
          if (log.isDebugEnabled()) log.debug("Node {} trying to join election at the head", id);
          List<String> nodes = OverseerTaskProcessor.getSortedElectionNodes(zkClient, shardsElectZkPath);
          if(nodes.size() <2){
            leaderSeqPath = zkClient.create(shardsElectZkPath + "/" + id + "-n_", null,
                    CreateMode.EPHEMERAL_SEQUENTIAL, true);
          } else {
            String firstInLine = nodes.get(1);
            log.debug("The current head: {}", firstInLine);
            Matcher m = LEADER_SEQ.matcher(firstInLine);
            if (!m.matches()) {
              throw new IllegalStateException("Could not find regex match in:"
                      + firstInLine);
            }
            leaderSeqPath = shardsElectZkPath + "/" + id + "-n_"+ m.group(1);
            zkClient.create(leaderSeqPath, null, CreateMode.EPHEMERAL, false);
          }
        } else {
          if (log.isDebugEnabled()) log.debug("create ephem election node {}", shardsElectZkPath + "/" + id + "-n_");
              leaderSeqPath = zkClient.create(shardsElectZkPath + "/" + id + "-n_", null,
                      CreateMode.EPHEMERAL_SEQUENTIAL, false);
        }

        if (log.isDebugEnabled()) log.debug("Joined leadership election with path: {}", leaderSeqPath);
        context.leaderSeqPath = leaderSeqPath;
        cont = false;
      } catch (ConnectionLossException e) {
        // we don't know if we made our node or not...
        List<String> entries = zkClient.getChildren(shardsElectZkPath, null, true);

        boolean foundId = false;
        for (String entry : entries) {
          String nodeId = getNodeId(entry);
          if (id.equals(nodeId)) {
            // we did create our node...
            foundId  = true;
            break;
          }
        }
        if (!foundId) {
          cont = true;
          if (tries++ > 5) {
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                    "", e);
          }
        }

      } catch (KeeperException.NoNodeException e) {
        // we must have failed in creating the election node - someone else must
        // be working on it, lets try again
        log.info("No node found during election {} " + e.getMessage(), e.getPath());
        if (tries++ > 5) {
          log.error("No node found during election {} " + e.getMessage(), e.getPath());
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        }
        cont = true;
      }
    }

    int seq = getSeq(context.leaderSeqPath);

    boolean tryagain = checkIfIamLeader(context, replacement);

    if (tryagain) {
      Thread.sleep(100);
      tryagain = checkIfIamLeader(context, replacement);
    }

    if (tryagain) {
      Thread.sleep(100);
      checkIfIamLeader(context, replacement);
    }


//    boolean tryagain = false;
//    while (tryagain) {
//      tryagain = checkIfIamLeader(context, replacement);
//      if (tryagain) {
//        Thread.sleep(250);
//      }
//    }

    return false;
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(watcher);
    this.isClosed = true;
  }

  private class ElectionWatcher implements Watcher, Closeable {
    final String myNode, watchedNode;
    final ElectionContext context;

    private volatile boolean canceled = false;

    private ElectionWatcher(String myNode, String watchedNode, int seq, ElectionContext context) {
      this.myNode = myNode;
      this.watchedNode = watchedNode;
      this.context = context;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      if (canceled) {
        if (log.isDebugEnabled()) log.debug("This watcher is not active anymore {}", myNode);
        return;
      }
      try {
        // am I the next leader?
        boolean tryagain = checkIfIamLeader(context, true);
        if (tryagain) {
          Thread.sleep(100);
          tryagain = checkIfIamLeader(context, true);
        }

        if (tryagain) {
          Thread.sleep(100);
          checkIfIamLeader(context, true);
        }
      } catch (AlreadyClosedException | InterruptedException e) {
        log.info("Already shutting down");
        return;
      } catch (Exception e) {
        log.error("Exception in election", e);
        return;
      }
    }

    @Override
    public void close() throws IOException {
      SolrZooKeeper zk = zkClient.getSolrZooKeeper();
      if (zk != null) {
        try {
          zk.removeWatches(context.leaderSeqPath, this, WatcherType.Any, true);
        } catch (InterruptedException e) {
          log.info("Interrupted removing leader watch");
        } catch (KeeperException e) {
          log.error("Exception removing leader watch", e);
        }
      }
      canceled = true;
    }
  }

  /**
   * Set up any ZooKeeper nodes needed for leader election.
   */
  public void setup(final ElectionContext context) {
    this.context = context;
  }

  /**
   * Sort n string sequence list.
   */
  public static void sortSeqs(List<String> seqs) {
    Collections.sort(seqs, Comparator.comparingInt(LeaderElector::getSeq).thenComparing(o -> o));
  }

  void retryElection(ElectionContext context, boolean joinAtHead) throws KeeperException, InterruptedException, IOException {
    ElectionWatcher watcher = this.watcher;
    if (electionContexts != null) {
      ElectionContext prevContext = electionContexts.put(contextKey, context);
      if (prevContext != null) {
        prevContext.close();
      }
    }
    if (watcher != null) watcher.close();
    this.context.close();
    this.context = context;
    joinElection(context, true, joinAtHead);
  }

}
