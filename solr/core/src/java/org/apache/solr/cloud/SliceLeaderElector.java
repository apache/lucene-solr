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
 * Per slice Leader Election process. This class contains the logic by which a
 * leader shard for a slice is chosen. First call
 * {@link #setupForSlice(String, String)} to ensure the election process is
 * init'd for a new slice. Next call
 * {@link #joinElection(String, String, String)} to start the leader election.
 * 
 * The implementation follows the classic ZooKeeper recipe of creating an
 * ephemeral, sequential node for each shard and then looking at the set of such
 * nodes - if the created node is the lowest sequential node, the shard that
 * created the node is the leader. If not, the shard puts a watch on the next
 * lowest node it finds, and if that node goes down, starts the whole process
 * over by checking if it's the lowest sequential node, etc.
 * 
 */
public class SliceLeaderElector {
  private static Logger log = LoggerFactory.getLogger(SliceLeaderElector.class);
  
  private static final String LEADER_NODE = "/leader";
  
  private static final String ELECTION_NODE = "/election";
  
  private final static Pattern LEADER_SEQ = Pattern.compile(".*?/?n_(\\d+)");
  
  private SolrZkClient zkClient;
  
  public SliceLeaderElector(SolrZkClient zkClient) {
    this.zkClient = zkClient;
  }
  
  /**
   * Check if the shard with the given n_* sequence number is the slice leader.
   * If it is, set the leaderId on the slice leader zk node. If it is not, start
   * watching the shard that is in line before this one - if it goes down, check
   * if this shard is the leader again.
   * 
   * @param shardId
   * @param collection
   * @param seq
   * @param leaderId
   * @throws KeeperException
   * @throws InterruptedException
   * @throws UnsupportedEncodingException
   */
  private void checkIfIamLeader(final String shardId, final String collection,
      final int seq, final String leaderId) throws KeeperException,
      InterruptedException, UnsupportedEncodingException {
    // get all other numbers...
    String holdElectionPath = getElectionPath(shardId, collection)
        + ELECTION_NODE;
    List<String> seqs = zkClient.getChildren(holdElectionPath, null);
    sortSeqs(seqs);
    List<Integer> intSeqs = getSeqs(seqs);
    if (seq <= intSeqs.get(0)) {
      runIamLeaderProcess(shardId, collection, leaderId);
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
                  checkIfIamLeader(shardId, collection, seq, leaderId);
                } catch (UnsupportedEncodingException e) {
                  log.error("", e);
                  throw new ZooKeeperException(
                      SolrException.ErrorCode.SERVER_ERROR, "", e);
                } catch (KeeperException e) {
                  log.error("", e);
                  throw new ZooKeeperException(
                      SolrException.ErrorCode.SERVER_ERROR, "", e);
                } catch (InterruptedException e) {
                  // Restore the interrupted status
                  Thread.currentThread().interrupt();
                  log.warn("", e);
                  throw new ZooKeeperException(
                      SolrException.ErrorCode.SERVER_ERROR, "", e);
                }
                
              }
            }, null);
      } catch (KeeperException e) {
        // we couldn't set our watch - the node before us may already be down?
        // we need to check if we are the leader again
        checkIfIamLeader(shardId, collection, seq, leaderId);
      }
    }
  }

  private void runIamLeaderProcess(final String shardId,
      final String collection, final String leaderId) throws KeeperException,
      InterruptedException, UnsupportedEncodingException {
    String currentLeaderZkPath = getElectionPath(shardId, collection)
        + LEADER_NODE;
    zkClient.makePath(currentLeaderZkPath + "/" + leaderId,  CreateMode.EPHEMERAL);
  }
  
  /**
   * /collections/{collection}/leader_elect/{shard_id}/
   * 
   * @param shardId
   * @param collection
   * @return
   */
  private String getElectionPath(String shardId, String collection) {
    return ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
        + ZkStateReader.LEADER_ELECT_ZKNODE + "/" + shardId;
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
   * Begin participating in the election process. Gets a new sequential number
   * and begins watching the node with the sequence number before it, unless it
   * is the lowest number, in which case, initiates the leader process. If the
   * node that is watched goes down, check if we are the new lowest node, else
   * watch the next lowest numbered node.
   * 
   * @param shardId
   * @param collection
   * @param shardZkNodeName
   * @return sequential node number
   * @throws KeeperException
   * @throws InterruptedException
   * @throws UnsupportedEncodingException
   */
  public int joinElection(String shardId, String collection,
      String shardZkNodeName) throws KeeperException, InterruptedException,
      UnsupportedEncodingException {
    final String shardsElectZkPath = getElectionPath(shardId, collection)
        + SliceLeaderElector.ELECTION_NODE;
    
    String leaderSeqPath = null;
    boolean cont = true;
    int tries = 0;
    while (cont) {
      try {
        leaderSeqPath = zkClient.create(shardsElectZkPath + "/n_", null,
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
    checkIfIamLeader(shardId, collection, seq, shardZkNodeName);
    
    return seq;
  }
  
  /**
   * Set up any ZooKeeper nodes needed per shardId (slice) for leader election.
   * 
   * @param shardId
   * @param collection
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void setupForSlice(final String shardId, final String collection)
      throws InterruptedException, KeeperException {
    String shardsElectZkPath = getElectionPath(shardId, collection)
        + SliceLeaderElector.ELECTION_NODE;
    String currentLeaderZkPath = getElectionPath(shardId, collection)
        + SliceLeaderElector.LEADER_NODE;
    
    try {
      
      // leader election node
      if (!zkClient.exists(shardsElectZkPath)) {
        
        // make new leader election node
        zkClient.makePath(shardsElectZkPath, CreateMode.PERSISTENT, null);
        
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
