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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Overseer Election process. Simplify this.
 */
public class OverseerElector {
  private static Logger log = LoggerFactory.getLogger(OverseerElector.class);
  
  private static final String LEADER_NODE = "/leader";
  
  private static final String ELECTION_NODE = "/election";
  
  private final static Pattern LEADER_SEQ = Pattern.compile(".*?/?n_(\\d+)");

  private static final String OVERSEER_ELECT_ZKNODE = "/overseer_elect";
  
  private SolrZkClient zkClient;
  
  public OverseerElector(SolrZkClient zkClient) {
    this.zkClient = zkClient;
  }
  
  /**
   * Check if this instance is the leader. If it is, set the leaderId on the
   * overseer leader zk node. If it is not, start watching the node that is in
   * line before this one - if it goes down, check if this node is the leader
   * again.
   * 
   * @param collection
   * @param seq
   * @param leaderId
   * @param props
   * @throws KeeperException
   * @throws InterruptedException
   * @throws IOException
   * @throws UnsupportedEncodingException
   */
  private void checkIfIamLeader(final int seq, final String leaderId)
      throws KeeperException, InterruptedException, IOException {
    // get all other numbers...
    String holdElectionPath = getElectionPath() + ELECTION_NODE;
    List<String> seqs = zkClient.getChildren(holdElectionPath, null);
    sortSeqs(seqs);
    List<Integer> intSeqs = getSeqs(seqs);
    if (seq <= intSeqs.get(0)) {
      runIamLeaderProcess(leaderId);
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
      
      try {
        zkClient.getData(holdElectionPath + "/" + seqs.get(i - 2),
            new Watcher() {
              
              @Override
              public void process(WatchedEvent event) {
                // am I the next leader?
                try {
                  checkIfIamLeader(seq, leaderId);
                } catch (KeeperException e) {
                  log.warn("", e);
                  
                } catch (InterruptedException e) {
                  // Restore the interrupted status
                  Thread.currentThread().interrupt();
                  log.warn("", e);
                } catch (IOException e) {
                  log.warn("", e);
                }
              }
              
            }, null);
      } catch (KeeperException e) {
        // we couldn't set our watch - the node before us may already be down?
        // we need to check if we are the leader again
        checkIfIamLeader(seq, leaderId);
      }
    }
  }
  
  private void runIamLeaderProcess(final String leaderId)
      throws KeeperException, InterruptedException, IOException {
    String currentLeaderZkPath = getElectionPath() + LEADER_NODE;
    zkClient.makePath(currentLeaderZkPath + "/" + leaderId,
        CreateMode.EPHEMERAL);
    
    // start overseer
    Overseer overseer = new Overseer(zkClient);
  }
  
  /**
   * /overseer_election
   * 
   * @return
   */
  private String getElectionPath() {
    return OVERSEER_ELECT_ZKNODE;
  }
  
  /**
   * Returns int given String of form n_0000000001 or n_0000000003, etc.
   * 
   * @param nStringSequence
   * @return
   */
  private int getSeq(String nStringSequence) {
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
  
  /**
   * Returns int list given list of form n_0000000001, n_0000000003, etc.
   * 
   * @param seqs
   * @return
   */
  private List<Integer> getSeqs(List<String> seqs) {
    List<Integer> intSeqs = new ArrayList<Integer>(seqs.size());
    for (String seq : seqs) {
      intSeqs.add(getSeq(seq));
    }
    return intSeqs;
  }
  
  /**
   * Begin participating in the overseer election process. Gets a new sequential
   * number and begins watching the node with the sequence number before it,
   * unless it is the lowest number, in which case, initiates the leader
   * process. If the node that is watched goes down, check if we are the new
   * lowest node, else watch the next lowest numbered node.
   * 
   * @param collection
   * @param shardZkNodeName
   * @param props
   * @return sequential node number
   * @throws KeeperException
   * @throws InterruptedException
   * @throws IOException
   * @throws UnsupportedEncodingException
   */
  public int joinElection(String shardZkNodeName) throws KeeperException,
      InterruptedException, IOException {
    final String electPath = getElectionPath()
        + OverseerElector.ELECTION_NODE;
    
    String leaderSeqPath = null;
    boolean cont = true;
    int tries = 0;
    while (cont) {
      try {
        
        leaderSeqPath = zkClient.create(electPath + "/n_", null,
            CreateMode.EPHEMERAL_SEQUENTIAL);
        cont = false;
      } catch (KeeperException.NoNodeException e) {
        // we must have failed in creating the election node - someone else must
        // be working on it, lets try again
        if (tries++ > 9) {
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        }
        cont = true;
        Thread.sleep(50);
      }
    }
    int seq = getSeq(leaderSeqPath);
    checkIfIamLeader(seq, shardZkNodeName);
    
    return seq;
  }
  
  /**
   * Set up any ZooKeeper nodes needed for overseer leader election.
   * 
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void setupForElection()
      throws InterruptedException, KeeperException {
    String electPath = getElectionPath()
        + OverseerElector.ELECTION_NODE;
    String currentLeaderZkPath = getElectionPath()
        + OverseerElector.LEADER_NODE;
    
    try {
      
      // leader election node
      if (!zkClient.exists(electPath)) {
        
        // make new leader election node
        zkClient.makePath(electPath, CreateMode.PERSISTENT, null);
        
      }
    } catch (KeeperException e) {
      // its okay if another beats us creating the node
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        throw e;
      }
    }
    
    try {
      
      // current leader node
      if (!zkClient.exists(currentLeaderZkPath)) {
        
        // make new current leader node
        zkClient.makePath(currentLeaderZkPath, CreateMode.PERSISTENT, null);
        
      }
    } catch (KeeperException e) {
      // its okay if another beats us creating the node
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        throw e;
      }
    }
  }
  
  /**
   * Sort n string sequence list.
   * 
   * @param seqs
   */
  private void sortSeqs(List<String> seqs) {
    Collections.sort(seqs, new Comparator<String>() {
      
      @Override
      public int compare(String o1, String o2) {
        return Integer.valueOf(getSeq(o1)).compareTo(
            Integer.valueOf(getSeq(o2)));
      }
    });
  }
}
