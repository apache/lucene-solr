package org.apache.solr.cloud;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZooKeeperException;
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
public  class LeaderElector {
  private static Logger log = LoggerFactory.getLogger(LeaderElector.class);
  
  static final String ELECTION_NODE = "/election";
  
  public final static Pattern LEADER_SEQ = Pattern.compile(".*?/?.*?-n_(\\d+)");
  private final static Pattern SESSION_ID = Pattern.compile(".*?/?(.*?-.*?)-n_\\d+");
  private final static Pattern  NODE_NAME = Pattern.compile(".*?/?(.*?-)(.*?)-n_\\d+");

  protected SolrZkClient zkClient;
  
  private ZkCmdExecutor zkCmdExecutor;

  private volatile ElectionContext context;

  private ElectionWatcher watcher;

  public LeaderElector(SolrZkClient zkClient) {
    this.zkClient = zkClient;
    zkCmdExecutor = new ZkCmdExecutor(zkClient.getZkClientTimeout());
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
  private void checkIfIamLeader(final int seq, final ElectionContext context, boolean replacement) throws KeeperException,
      InterruptedException, IOException {
    context.checkIfIamLeaderFired();
    // get all other numbers...
    final String holdElectionPath = context.electionPath + ELECTION_NODE;
    List<String> seqs = zkClient.getChildren(holdElectionPath, null, true);

    sortSeqs(seqs);
    List<Integer> intSeqs = getSeqs(seqs);
    if (intSeqs.size() == 0) {
      log.warn("Our node is no longer in line to be leader");
      return;
    }
    if (seq <= intSeqs.get(0)) {
      if(seq == intSeqs.get(0) && !context.leaderSeqPath.equals(holdElectionPath+"/"+seqs.get(0)) ) {//somebody else already  became the leader with the same sequence id , not me
        log.info("was going be leader {} , seq(0) {}",context.leaderSeqPath,holdElectionPath+"/"+seqs.get(0));//but someone else jumped the line
        retryElection(context,false);//join at the tail again
        return;
      }
      // first we delete the node advertising the old leader in case the ephem is still there
      try {
        zkClient.delete(context.leaderPath, -1, true);
      }catch (KeeperException.NoNodeException nne){
        //no problem
      }catch (InterruptedException e){
        throw e;
      } catch (Exception e) {
        //failed to delete the leader node
        log.error("leader elect delete error",e);
        retryElection(context, false);
        return;
        // fine
      }

      try {
        runIamLeaderProcess(context, replacement);
      } catch (KeeperException.NodeExistsException e) {
        log.error("node exists",e);
        retryElection(context, false);
        return;
      }
    } else {
      // I am not the leader - watch the node below me
      int i = 1;
      for (; i < intSeqs.size(); i++) {
        int s = intSeqs.get(i);
        if (seq < s) {
          // we found who we come before - watch the guy in front
          break;
        }
      }
      int index = i - 2;
      if (index < 0) {
        log.warn("Our node is no longer in line to be leader");
        return;
      }
      try {
        String watchedNode = holdElectionPath + "/" + seqs.get(index);
        zkClient.getData(watchedNode, watcher = new ElectionWatcher(context.leaderSeqPath , watchedNode,seq, context) , null, true);
      } catch (KeeperException.SessionExpiredException e) {
        throw e;
      } catch (KeeperException e) {
        log.warn("Failed setting watch", e);
        // we couldn't set our watch - the node before us may already be down?
        // we need to check if we are the leader again
        checkIfIamLeader(seq, context, true);
      }
    }
  }

  // TODO: get this core param out of here
  protected void runIamLeaderProcess(final ElectionContext context, boolean weAreReplacement) throws KeeperException,
      InterruptedException, IOException {
    context.runLeaderProcess(weAreReplacement,0);
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
    String result;
    Matcher m = NODE_NAME.matcher(nStringSequence);
    if (m.matches()) {
      result = m.group(2);
    } else {
      throw new IllegalStateException("Could not find regex match in:"
          + nStringSequence);
    }
    return result;

  }
  
  /**
   * Returns int list given list of form n_0000000001, n_0000000003, etc.
   * 
   * @return int seqs
   */
  private List<Integer> getSeqs(List<String> seqs) {
    List<Integer> intSeqs = new ArrayList<>(seqs.size());
    for (String seq : seqs) {
      intSeqs.add(getSeq(seq));
    }
    return intSeqs;
  }
  public int joinElection(ElectionContext context, boolean replacement) throws KeeperException, InterruptedException, IOException {
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
  public int joinElection(ElectionContext context, boolean replacement,boolean joinAtHead) throws KeeperException, InterruptedException, IOException {
    context.joinedElectionFired();
    
    final String shardsElectZkPath = context.electionPath + LeaderElector.ELECTION_NODE;
    
    long sessionId = zkClient.getSolrZooKeeper().getSessionId();
    String id = sessionId + "-" + context.id;
    String leaderSeqPath = null;
    boolean cont = true;
    int tries = 0;
    while (cont) {
      try {
        if(joinAtHead){
          log.info("node {} Trying to join election at the head ", id);
          List<String> nodes = OverseerCollectionProcessor.getSortedElectionNodes(zkClient, shardsElectZkPath);
          if(nodes.size() <2){
            leaderSeqPath = zkClient.create(shardsElectZkPath + "/" + id + "-n_", null,
                CreateMode.EPHEMERAL_SEQUENTIAL, false);
          } else {
            String firstInLine = nodes.get(1);
            log.info("The current head: {}", firstInLine);
            Matcher m = LEADER_SEQ.matcher(firstInLine);
            if (!m.matches()) {
              throw new IllegalStateException("Could not find regex match in:"
                  + firstInLine);
            }
            leaderSeqPath = shardsElectZkPath + "/" + id + "-n_"+ m.group(1);
            zkClient.create(leaderSeqPath, null, CreateMode.EPHEMERAL, false);
            log.info("Joined at the head  {}", leaderSeqPath );

          }
        } else {
          leaderSeqPath = zkClient.create(shardsElectZkPath + "/" + id + "-n_", null,
              CreateMode.EPHEMERAL_SEQUENTIAL, false);
        }

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
          if (tries++ > 20) {
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          }
          try {
            Thread.sleep(50);
          } catch (InterruptedException e2) {
            Thread.currentThread().interrupt();
          }
        }

      } catch (KeeperException.NoNodeException e) {
        // we must have failed in creating the election node - someone else must
        // be working on it, lets try again
        if (tries++ > 20) {
          context = null;
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        }
        cont = true;
        try {
          Thread.sleep(50);
        } catch (InterruptedException e2) {
          Thread.currentThread().interrupt();
        }
      }
    }
    int seq = getSeq(leaderSeqPath);
    checkIfIamLeader(seq, context, replacement);
    
    return seq;
  }

  private class ElectionWatcher implements Watcher {
    final String myNode,watchedNode;
    final int seq;
    final ElectionContext context;

    private boolean canceled = false;

    private ElectionWatcher(String myNode, String watchedNode, int seq, ElectionContext context) {
      this.myNode = myNode;
      this.watchedNode = watchedNode;
      this.seq = seq;
      this.context = context;
    }

    void cancel(String leaderSeqPath){
      canceled = true;

    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events,
      // and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      if (canceled) {
        log.info("This watcher is not active anymore {}", myNode);
        try {
          zkClient.delete(myNode, -1, true);
        } catch (KeeperException.NoNodeException nne) {
          // expected . don't do anything
        } catch (Exception e) {
          log.warn("My watched node still exists and can't remove " + myNode, e);
        }
        return;
      }
      try {
        // am I the next leader?
        checkIfIamLeader(seq, context, true);
      } catch (Exception e) {
        log.warn("", e);
      }
    }
  }

  /**
   * Set up any ZooKeeper nodes needed for leader election.
   */
  public void setup(final ElectionContext context) throws InterruptedException,
      KeeperException {
    String electZKPath = context.electionPath + LeaderElector.ELECTION_NODE;
    
    zkCmdExecutor.ensureExists(electZKPath, zkClient);
    this.context = context;
  }
  
  /**
   * Sort n string sequence list.
   */
  public static void sortSeqs(List<String> seqs) {
    Collections.sort(seqs, new Comparator<String>() {
      
      @Override
      public int compare(String o1, String o2) {
        int i = Integer.valueOf(getSeq(o1)).compareTo(
            Integer.valueOf(getSeq(o2)));
        return i == 0 ? o1.compareTo(o2) : i ;
      }
    });
  }

  void retryElection(ElectionContext context, boolean joinAtHead) throws KeeperException, InterruptedException, IOException {
    ElectionWatcher watcher = this.watcher;
    ElectionContext ctx = context.copy();
    if(watcher!= null) watcher.cancel(this.context.leaderSeqPath);
    this.context.cancelElection();
    this.context = ctx;
    joinElection(ctx, true, joinAtHead);
  }

}
