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

package org.apache.solr.client.solrj.cloud;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;

/**
 * Hold values of terms, this class is immutable. Create a new instance for every mutation
 */
public class ShardTerms implements MapWriter {
  private static final String RECOVERING_TERM_SUFFIX = "_recovering";
  private final Map<String, Long> values;
  private final long maxTerm;
  // ZK node version
  private final int version;

  public ShardTerms () {
    this(new HashMap<>(), 0);
  }

  public ShardTerms(ShardTerms newTerms, int version) {
    this(newTerms.values, version);
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    values.forEach(ew.getBiConsumer());
  }

  public ShardTerms(Map<String, Long> values, int version) {
    this.values = values;
    this.version = version;
    if (values.isEmpty()) this.maxTerm = 0;
    else this.maxTerm = Collections.max(values.values());
  }

  /**
   * Can {@code coreNodeName} become leader?
   * @param coreNodeName of the replica
   * @return true if {@code coreNodeName} can become leader, false if otherwise
   */
  public boolean canBecomeLeader(String coreNodeName) {
    return haveHighestTermValue(coreNodeName) && !values.containsKey(recoveringTerm(coreNodeName));
  }

  /**
   * Is {@code coreNodeName}'s term highest?
   * @param coreNodeName of the replica
   * @return true if term of {@code coreNodeName} is highest
   */
  public boolean haveHighestTermValue(String coreNodeName) {
    if (values.isEmpty()) return true;
    return values.getOrDefault(coreNodeName, 0L) == maxTerm;
  }

  public Long getTerm(String coreNodeName) {
    return values.get(coreNodeName);
  }

  /**
   * Return a new {@link ShardTerms} in which term of {@code leader} is higher than {@code replicasNeedingRecovery}
   * @param leader coreNodeName of leader
   * @param replicasNeedingRecovery set of replicas in which their terms should be lower than leader's term
   * @return null if term of {@code leader} is already higher than {@code replicasNeedingRecovery}
   */
  public ShardTerms increaseTerms(String leader, Set<String> replicasNeedingRecovery) {
    if (!values.containsKey(leader)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can not find leader's term " + leader);
    }

    boolean saveChanges = false;
    boolean foundReplicasInLowerTerms = false;

    HashMap<String, Long> newValues = new HashMap<>(values);
    long leaderTerm = newValues.get(leader);
    for (Map.Entry<String, Long> entry : newValues.entrySet()) {
      String key = entry.getKey();
      if (replicasNeedingRecovery.contains(key)) foundReplicasInLowerTerms = true;
      if (Objects.equals(entry.getValue(), leaderTerm)) {
        if(skipIncreaseTermOf(key, replicasNeedingRecovery)) {
          saveChanges = true; // if we don't skip anybody, then there's no reason to increment
        } else {
          entry.setValue(leaderTerm + 1);
        }
      }
    }

    // We should skip the optimization if there are no replicasNeedingRecovery present in local terms,
    // this may indicate that the current value is stale
    if (!saveChanges && foundReplicasInLowerTerms) return null;
    return new ShardTerms(newValues, version);
  }

  private boolean skipIncreaseTermOf(String key, Set<String> replicasNeedingRecovery) {
    if (key.endsWith(RECOVERING_TERM_SUFFIX)) {
      key = key.substring(0, key.length() - RECOVERING_TERM_SUFFIX.length());
    }
    return replicasNeedingRecovery.contains(key);
  }

  /**
   * Return a new {@link ShardTerms} in which highest terms are not zero
   * @return null if highest terms are already larger than zero
   */
  public ShardTerms ensureHighestTermsAreNotZero() {
    if (maxTerm > 0) return null;
    else {
      HashMap<String, Long> newValues = new HashMap<>(values);
      for (String replica : values.keySet()) {
        newValues.put(replica, 1L);
      }
      return new ShardTerms(newValues, version);
    }
  }

  /**
   * Return a new {@link ShardTerms} in which terms for the {@code coreNodeName} are removed
   * @param coreNodeName of the replica
   * @return null if term of {@code coreNodeName} is already not exist
   */
  public ShardTerms removeTerm(String coreNodeName) {
    if (!values.containsKey(recoveringTerm(coreNodeName)) && !values.containsKey(coreNodeName)) {
      return null;
    }

    HashMap<String, Long> newValues = new HashMap<>(values);
    newValues.remove(coreNodeName);
    newValues.remove(recoveringTerm(coreNodeName));

    return new ShardTerms(newValues, version);
  }

  /**
   * Return a new {@link ShardTerms} in which the associate term of {@code coreNodeName} is not null
   * @param coreNodeName of the replica
   * @return null if term of {@code coreNodeName} is already exist
   */
  public ShardTerms registerTerm(String coreNodeName) {
    if (values.containsKey(coreNodeName)) return null;

    HashMap<String, Long> newValues = new HashMap<>(values);
    newValues.put(coreNodeName, 0L);
    return new ShardTerms(newValues, version);
  }

  /**
   * Return a new {@link ShardTerms} in which the associate term of {@code coreNodeName} is equal to zero,
   * creating it if it does not previously exist.
   * @param coreNodeName of the replica
   * @return null if the term of {@code coreNodeName} already exists and is zero
   */
  public ShardTerms setTermToZero(String coreNodeName) {
    if (values.getOrDefault(coreNodeName, -1L) == 0) {
      return null;
    }
    HashMap<String, Long> newValues = new HashMap<>(values);
    newValues.put(coreNodeName, 0L);
    return new ShardTerms(newValues, version);
  }

  /**
   * Return a new {@link ShardTerms} in which the term of {@code coreNodeName} is max
   * @param coreNodeName of the replica
   * @return null if term of {@code coreNodeName} is already maximum
   */
  public ShardTerms setTermEqualsToLeader(String coreNodeName) {
    if (values.get(coreNodeName) == maxTerm) return null;

    HashMap<String, Long> newValues = new HashMap<>(values);
    newValues.put(coreNodeName, maxTerm);
    newValues.remove(recoveringTerm(coreNodeName));
    return new ShardTerms(newValues, version);
  }

  public long getMaxTerm() {
    return maxTerm;
  }

  /**
   * Mark {@code coreNodeName} as recovering
   * @param coreNodeName of the replica
   * @return null if {@code coreNodeName} is already marked as doing recovering
   */
  public ShardTerms startRecovering(String coreNodeName) {
    if (values.get(coreNodeName) == maxTerm)
      return null;

    HashMap<String, Long> newValues = new HashMap<>(values);
    if (!newValues.containsKey(recoveringTerm(coreNodeName))) {
      long currentTerm = newValues.getOrDefault(coreNodeName, 0L);
      // by keeping old term, we will have more information in leader election
      newValues.put(recoveringTerm(coreNodeName), currentTerm);
    }
    newValues.put(coreNodeName, maxTerm);
    return new ShardTerms(newValues, version);
  }

  /**
   * Mark {@code coreNodeName} as finished recovering
   * @param coreNodeName of the replica
   * @return null if term of {@code coreNodeName} is already finished doing recovering
   */
  public ShardTerms doneRecovering(String coreNodeName) {
    if (!values.containsKey(recoveringTerm(coreNodeName))) {
      return null;
    }

    HashMap<String, Long> newValues = new HashMap<>(values);
    newValues.remove(recoveringTerm(coreNodeName));
    return new ShardTerms(newValues, version);
  }

  public static String recoveringTerm(String coreNodeName) {
    return coreNodeName + RECOVERING_TERM_SUFFIX;
  }

  @Override
  public String toString() {
    return "Terms{" +
        "values=" + values +
        ", version=" + version +
        '}';
  }

  public int getVersion() {
    return version;
  }

  public Map<String, Long> getTerms() {
    return new HashMap<>(this.values);
  }

  public boolean isRecovering(String name) {
    return values.containsKey(recoveringTerm(name));
  }
}
