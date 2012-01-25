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
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader Election process. This class contains the logic by which a
 * leader is chosen. First call * {@link #setup(ElectionContext)} to ensure
 * the election process is init'd. Next call
 * {@link #joinElection(ElectionContext)} to start the leader election.
 * 
 * The implementation follows the classic ZooKeeper recipe of creating an
 * ephemeral, sequential node for each candidate and then looking at the set
 * of such nodes - if the created node is the lowest sequential node, the
 * candidate that created the node is the leader. If not, the candidate puts
 * a watch on the next lowest node it finds, and if that node goes down, 
 * starts the whole process over by checking if it's the lowest sequential node, etc.
 * 
 * TODO: now we could just reuse the lock package code for leader election
 */
public  class LeaderElector {
  private static Logger log = LoggerFactory.getLogger(LeaderElector.class);
  
  private static final String ELECTION_NODE = "/election";
  
  private final static Pattern LEADER_SEQ = Pattern.compile(".*?/?.*?-n_(\\d+)");
  private final static Pattern SESSION_ID = Pattern.compile(".*?/?(.*?-.*?)-n_\\d+");
  
  protected SolrZkClient zkClient;
  
  private ZkCmdExecutor zkCmdExecutor = new ZkCmdExecutor();
  
  public LeaderElector(SolrZkClient zkClient) {
    this.zkClient = zkClient;
  }
  
  /**
   * Check if the candidate with the given n_* sequence number is the leader.
   * If it is, set the leaderId on the leader zk node. If it is not, start
   * watching the candidate that is in line before this one - if it goes down, check
   * if this candidate is the leader again.
   * @param leaderSeqPath 
   * 
   * @param seq
   * @param context 
   * @param replacement has someone else been the leader already?
   * @throws KeeperException
   * @throws InterruptedException
   * @throws IOException 
   * @throws UnsupportedEncodingException
   */
  private void checkIfIamLeader(final String leaderSeqPath, final int seq, final ElectionContext context, boolean replacement) throws KeeperException,
      InterruptedException, IOException {
    // get all other numbers...
    final String holdElectionPath = context.electionPath + ELECTION_NODE;
    List<String> seqs = zkClient.getChildren(holdElectionPath, null, true);
    
    sortSeqs(seqs);
    List<Integer> intSeqs = getSeqs(seqs);
    if (seq <= intSeqs.get(0)) {
      runIamLeaderProcess(leaderSeqPath, context, replacement);
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
        zkClient.getData(holdElectionPath + "/" + seqs.get(index),
            new Watcher() {
              
              @Override
              public void process(WatchedEvent event) {
                // am I the next leader?
                try {
                  checkIfIamLeader(leaderSeqPath, seq, context, true);
                } catch (InterruptedException e) {
                  // Restore the interrupted status
                  Thread.currentThread().interrupt();
                  log.warn("", e);
                } catch (IOException e) {
                  log.warn("", e);
                } catch (Exception e) {
                  log.warn("", e);
                }
              }
              
            }, null, true);
      } catch (KeeperException.SessionExpiredException e) {
        throw e;
      } catch (KeeperException e) {
        // we couldn't set our watch - the node before us may already be down?
        // we need to check if we are the leader again
        checkIfIamLeader(leaderSeqPath, seq, context, true);
      }
    }
  }

  // TODO: get this core param out of here
  protected void runIamLeaderProcess(String leaderSeqPath, final ElectionContext context, boolean weAreReplacement) throws KeeperException,
      InterruptedException, IOException {

    context.runLeaderProcess(leaderSeqPath, weAreReplacement);
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
   * @param context
   * @param SolrCore - optional - sometimes null
   * @return sequential node number
   * @throws KeeperException
   * @throws InterruptedException
   * @throws IOException 
   * @throws UnsupportedEncodingException
   */
  public int joinElection(ElectionContext context) throws KeeperException, InterruptedException, IOException {
    final String shardsElectZkPath = context.electionPath + LeaderElector.ELECTION_NODE;
    
    long sessionId = zkClient.getSolrZooKeeper().getSessionId();
    String id = sessionId + "-" + context.id;
    String leaderSeqPath = null;
    boolean cont = true;
    int tries = 0;
    while (cont) {
      try {
        leaderSeqPath = zkClient.create(shardsElectZkPath + "/" + id + "-n_", null,
            CreateMode.EPHEMERAL_SEQUENTIAL, false);
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
          throw e;
        }

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
    checkIfIamLeader(leaderSeqPath, seq, context, false);
    
    return seq;
  }
  
  /**
   * Set up any ZooKeeper nodes needed for leader election.
   * 
   * @param shardId
   * @param collection
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void setup(final ElectionContext context) throws InterruptedException,
      KeeperException {
    String electZKPath = context.electionPath + LeaderElector.ELECTION_NODE;
    
    zkCmdExecutor.ensureExists(electZKPath, zkClient);
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
