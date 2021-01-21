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

import org.apache.solr.cloud.ZkController.ContextKey;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.SolrZooKeeper;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Leader Election process. This class contains the logic by which a
 * leader is chosen. First call * {@link #setup(ElectionContext)} to ensure
 * the election process is init'd. Next call
 * {@link #joinElection(boolean)} to start the leader election.
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
  private static final String JOINED = "j2";
  private static final String JOIN = "j1";
  private static final String CHECK_IF_LEADER = "lc";
  private static final String OUT_OF_ELECTION = "o";
  private static final String POT_LEADER = "pt";
  private static final String LEADER = "l";
  private static final String CLOSED = "c";
  private static final String WAITING_IN_ELECTION = "w";

  protected final SolrZkClient zkClient;
  private final ZkController zkController;

  private volatile ElectionContext context;

  private volatile ElectionWatcher watcher;

  private volatile boolean isClosed;
  private volatile Future<?> joinFuture;
  private volatile boolean isCancelled;

  private ExecutorService executor = ParWork.getExecutorService(1);

  private volatile String state = OUT_OF_ELECTION;

  //  public LeaderElector(SolrZkClient zkClient) {
//    this.zkClient = zkClient;
//    this.contextKey = null;
//    this.electionContexts = new ConcurrentHashMap<>(132, 0.75f, 50);
//  }

  public LeaderElector(ZkController zkController, ContextKey key) {

    this.zkClient = zkController.getZkClient();
    this.zkController = zkController;
    assert ObjectReleaseTracker.track(this);
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
  private synchronized boolean checkIfIamLeader(final ElectionContext context, boolean replacement) throws KeeperException,
          InterruptedException, IOException {
    //if (checkClosed(context)) return false;

    if (log.isDebugEnabled()) log.debug("Check if I am leader {}", context.getClass().getSimpleName());
    if (isClosed) {
      log.info("elector is closed, won't join election");
      return false;
    }

    executor.submit(() -> {
      context.checkIfIamLeaderFired();
    });

    state = CHECK_IF_LEADER;
    // get all other numbers...
    final String holdElectionPath = context.electionPath + ELECTION_NODE;
    List<String> seqs;
    try {
      seqs = zkClient.getChildren(holdElectionPath, null, true);
    } catch (KeeperException.SessionExpiredException e) {
      log.error("ZooKeeper session has expired");
      state = OUT_OF_ELECTION;
      return false;
    } catch (KeeperException.NoNodeException e) {
      log.info("the election node disappeared, check if we are the leader again");
      state = OUT_OF_ELECTION;
      return false;
    } catch (KeeperException e) {
      // we couldn't set our watch for some other reason, retry
      log.warn("Failed setting election watch, retrying {} {}", e.getClass().getName(), e.getMessage());
      state = OUT_OF_ELECTION;
      return true;
    } catch (Exception e) {
      // we couldn't set our watch for some other reason, retry
      log.error("Failed on election getchildren call {} {}", e.getClass().getName(), e.getMessage());
      state = OUT_OF_ELECTION;
      return true;
    }

    try {

      sortSeqs(seqs);

      String leaderSeqNodeName;
      try {
        leaderSeqNodeName = context.leaderSeqPath.substring(context.leaderSeqPath.lastIndexOf('/') + 1);
      } catch (NullPointerException e) {
        state = OUT_OF_ELECTION;
        if (log.isDebugEnabled()) log.debug("leaderSeqPath has been removed, bailing");
        return true;
      }
      if (!seqs.contains(leaderSeqNodeName)) {
        log.warn("Our node is no longer in line to be leader");
        state = OUT_OF_ELECTION;
        return false;
      }
      if (log.isDebugEnabled()) log.debug("The leader election node is {}", leaderSeqNodeName);
      if (leaderSeqNodeName.equals(seqs.get(0))) {
        // I am the leader
        if (log.isDebugEnabled()) log.debug("I am the potential leader {}, running leader process", context.leaderProps.getName());
        ElectionWatcher oldWatcher = watcher;
        if (oldWatcher != null) {
          oldWatcher.close();
        }

        if ((zkController != null && zkController.getCoreContainer().isShutDown())) {
          if (log.isDebugEnabled()) log.debug("Elector is closed, will not try and run leader processes");
          state = OUT_OF_ELECTION;
          return false;
        }

        state = POT_LEADER;
        runIamLeaderProcess(context, replacement);
        return false;

      } else {

        String toWatch = seqs.get(0);
        for (String node : seqs) {
          if (leaderSeqNodeName.equals(node)) {
            break;
          }
          toWatch = node;
        }
        try {
          String watchedNode = holdElectionPath + "/" + toWatch;

          log.info("I am not the leader (our path is ={}) - watch the node below me {} seqs={}", leaderSeqNodeName, watchedNode, seqs);

          ElectionWatcher oldWatcher = watcher;
          if (oldWatcher != null) {
            IOUtils.closeQuietly(oldWatcher);
          }

          watcher = new ElectionWatcher(context.leaderSeqPath, watchedNode, context);
          Stat exists = zkClient.exists(watchedNode, watcher);
          if (exists == null) {
            state = OUT_OF_ELECTION;
            return true;
          }

          state = WAITING_IN_ELECTION;
          if (log.isDebugEnabled()) log.debug("Watching path {} to know if I could be the leader, my node is {}", watchedNode, context.leaderSeqPath);

          return false;
        } catch (KeeperException.SessionExpiredException e) {
          state = OUT_OF_ELECTION;
          log.error("ZooKeeper session has expired");
          throw e;
        } catch (KeeperException.NoNodeException e) {
          log.info("the previous node disappeared, check if we are the leader again");

        } catch (KeeperException e) {
          // we couldn't set our watch for some other reason, retry
          log.warn("Failed setting election watch, retrying {} {}", e.getClass().getName(), e.getMessage());

        } catch (Exception e) {
          state = OUT_OF_ELECTION;
          // we couldn't set our watch for some other reason, retry
          log.error("Failed setting election watch {} {}", e.getClass().getName(), e.getMessage());
        }
      }

    } catch (KeeperException.SessionExpiredException e) {
      log.error("ZooKeeper session has expired");
      state = OUT_OF_ELECTION;
      return false;
    } catch (AlreadyClosedException e) {
      state = OUT_OF_ELECTION;
      return false;
    } catch (Exception e) {
      state = OUT_OF_ELECTION;
      return true;
    }

    state = OUT_OF_ELECTION;
    return true;
  }


  // TODO: get this core param out of here
  protected void runIamLeaderProcess(final ElectionContext context, boolean weAreReplacement) throws KeeperException,
          InterruptedException, IOException {
    if (state == CLOSED) {
      throw new AlreadyClosedException();
    }
    if (state == LEADER) {
      throw new IllegalStateException("Already in leader state");
    }

    boolean success = context.runLeaderProcess(context, weAreReplacement, 0);

    if (success) {
      state = LEADER;
    } else {
      state = OUT_OF_ELECTION;
    }
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

  public void joinElection(boolean replacement) {
    joinElection(replacement, false);
  }

  public void joinElection(boolean replacement,boolean joinAtHead) {
    if (!isClosed && !zkController.getCoreContainer().isShutDown() && !zkController.isDcCalled() && zkClient.isAlive()) {
      joinFuture = executor.submit(() -> {
        try {
          isCancelled = false;
          doJoinElection(context, replacement, joinAtHead);
        } catch (Exception e) {
          log.error("Exception trying to join election", e);
        }
      });
    }
  }

  /**
   * Begin participating in the election process. Gets a new sequential number
   * and begins watching the node with the sequence number before it, unless it
   * is the lowest number, in which case, initiates the leader process. If the
   * node that is watched goes down, check if we are the new lowest node, else
   * watch the next lowest numbered node.
   *
   */
  public synchronized void doJoinElection(ElectionContext context, boolean replacement,boolean joinAtHead) throws KeeperException, InterruptedException, IOException {
    //if (checkClosed(context)) return false;
    if (shouldRejectJoins() || state == CLOSED) {
      log.info("elector is closed, won't join election");
      throw new AlreadyClosedException();
    }

    if (state != OUT_OF_ELECTION) {
      throw new IllegalStateException("Expected " + OUT_OF_ELECTION + " but got " + state);
    }
    state = JOIN;

    isCancelled = false;

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
            leaderSeqPath = zkClient.create(shardsElectZkPath + "/" + id + "-n_", (byte[]) null,
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
            zkClient.create(leaderSeqPath, (byte[]) null, CreateMode.EPHEMERAL, false);
          }
        } else {
          if (log.isDebugEnabled()) log.debug("create ephem election node {}", shardsElectZkPath + "/" + id + "-n_");
              leaderSeqPath = zkClient.create(shardsElectZkPath + "/" + id + "-n_", (byte[]) null,
                      CreateMode.EPHEMERAL_SEQUENTIAL, false);
        }

        log.info("Joined leadership election with path: {}", leaderSeqPath);
        context.leaderSeqPath = leaderSeqPath;
        state = JOIN;
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

    if (log.isDebugEnabled()) log.debug("Do checkIfIamLeader");
    boolean tryagain = true;

    while (tryagain) {
      tryagain = checkIfIamLeader(context, replacement);
    }

//    boolean tryagain = false;
//    while (tryagain) {
//      tryagain = checkIfIamLeader(context, replacement);
//      if (tryagain) {
//        Thread.sleep(250);
//      }
//    }

  }

  private boolean shouldRejectJoins() {
    return zkController.getCoreContainer().isShutDown() || zkController.isDcCalled();
  }

  @Override
  public void close() throws IOException {
    assert ObjectReleaseTracker.release(this);
    state = CLOSED;
    this.isClosed = true;
    IOUtils.closeQuietly(watcher);
    if (context != null) {
      try {
        context.cancelElection();
      } catch (Exception e) {
        log.warn("Exception canceling election", e);
      }
    }
    try {
      if (joinFuture != null) {
        joinFuture.cancel(false);
      }
    } catch (NullPointerException e) {
      // okay
    }
  }

  public void cancel() {

    if (state == OUT_OF_ELECTION || state == CLOSED) {
      return;
    }

    state = OUT_OF_ELECTION;

    try {
      this.isCancelled = true;
      IOUtils.closeQuietly(watcher);
      if (context != null) {
        context.cancelElection();
      }
      Future<?> jf = joinFuture;
      if (jf != null) {
        jf.cancel(false);
//        if (!shouldRejectJoins()) {
//          try {
//            jf.get(500, TimeUnit.MILLISECONDS);
//
//          } catch (TimeoutException e) {
//
//          } catch (Exception e) {
//            log.error("Exception waiting for previous election attempt to finish {} {} cause={}", e.getClass().getSimpleName(), e.getMessage());
//          }
//        }

      }
    } catch (Exception e) {
      log.warn("Exception canceling election", e);
    }
  }

  public boolean isClosed() {
    return isClosed;
  }

  private class ElectionWatcher implements Watcher, Closeable {
    final String myNode, watchedNode;
    final ElectionContext context;

    private volatile boolean canceled = false;

    private ElectionWatcher(String myNode, String watchedNode, ElectionContext context) {
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

      if (log.isDebugEnabled()) log.debug("Got event on node we where watching in leader line {} watchedNode={}", myNode, watchedNode);

      if (isCancelled || isClosed) {
        if (log.isDebugEnabled()) log.debug("This watcher is not active anymore {} isCancelled={} isClosed={}", myNode, isCancelled, isClosed);
        return;
      }
      try {
        if (event.getType() == EventType.NodeDeleted) {
          // am I the next leader?
          boolean tryagain = true;
          while (tryagain) {
            tryagain = checkIfIamLeader(context, true);
          }
        } else {
          Stat exists = zkClient.exists(watchedNode, this);
          if (exists == null) {
            close();
            boolean tryagain = true;

            while (tryagain) {
              tryagain = checkIfIamLeader(context, true);
            }
          }
        }
        // we don't kick off recovery here, the leader sync will do that if necessary for its replicas
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
      try {
        zk.removeWatches(watchedNode, this, WatcherType.Any, true);
      } catch (Exception e) {
        log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
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

  void retryElection(boolean joinAtHead) {
    if (shouldRejectJoins()) {
      throw new AlreadyClosedException();
    }
    cancel();
    ElectionWatcher watcher = this.watcher;
    IOUtils.closeQuietly(watcher);
    IOUtils.closeQuietly(this);
    isCancelled = false;
    joinElection(true, joinAtHead);
  }

  public boolean isLeader() {
    return LEADER.equals(state);
  }
}
