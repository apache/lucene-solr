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

package org.apache.solr.common.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.apache.solr.cluster.api.SimpleMap;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.WrappedSimpleMap;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.VERSION;

/**
 * This represents the individual replica states in a collection
 * This is an immutable object. When states are modified, a new instance is constructed
 */
public class PerReplicaStates implements ReflectMapWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final char SEPARATOR = ':';
  //no:of times to retry in case of a CAS failure
  public static final int MAX_RETRIES = 5;


  //znode path where thisis loaded from
  @JsonProperty
  public final String path;

  // the child version of that znode
  @JsonProperty
  public final int cversion;

  //states of individual replicas
  @JsonProperty
  public final SimpleMap<State> states;

  private Boolean allActive;

  /**
   * Construct with data read from ZK
   * @param path path from where this is loaded
   * @param cversion the current child version of the znode
   * @param states the per-replica states (the list of all child nodes)
   */
  public PerReplicaStates(String path, int cversion, List<String> states) {
    this.path = path;
    this.cversion = cversion;
    Map<String, State> tmp = new LinkedHashMap<>();

    for (String state : states) {
      State rs = State.parse(state);
      if (rs == null) continue;
      State existing = tmp.get(rs.replica);
      if (existing == null) {
        tmp.put(rs.replica, rs);
      } else {
        tmp.put(rs.replica, rs.insert(existing));
      }
    }
    this.states = new WrappedSimpleMap<>(tmp);

  }

  /** Check and return if all replicas are ACTIVE
   */
  public boolean allActive() {
    if (this.allActive != null) return allActive;
    AtomicBoolean result = new AtomicBoolean(true);
    states.forEachEntry((r, s) -> {
      if (s.state != Replica.State.ACTIVE) result.set(false);
    });
    return this.allActive = result.get();
  }

  /**Get the changed replicas
   */
  public static Set<String> findModifiedReplicas(PerReplicaStates old, PerReplicaStates fresh) {
    Set<String> result = new HashSet<>();
    if (fresh == null) {
      old.states.forEachKey(result::add);
      return result;
    }
    old.states.forEachEntry((s, state) -> {
      // the state is modified or missing
      if (!Objects.equals(fresh.get(s) , state)) result.add(s);
    });
    fresh.states.forEachEntry((s, state) -> { if (old.get(s) == null ) result.add(s);
    });
    return result;
  }


  /**
   * Fetch the latest {@link PerReplicaStates} . It fetches data after checking the {@link Stat#getCversion()} of state.json.
   * If this is not modified, the same object is returned
   */
  public static PerReplicaStates fetch(String path, SolrZkClient zkClient, PerReplicaStates current) {
    try {
      if (current != null) {
        Stat stat = zkClient.exists(current.path, null, true);
        if (stat == null) return new PerReplicaStates(path, -1, Collections.emptyList());
        if (current.cversion == stat.getCversion()) return current;// not modifiedZkStateReaderTest
      }
      Stat stat = new Stat();
      List<String> children = zkClient.getChildren(path, null, stat, true);
      return new PerReplicaStates(path, stat.getCversion(), Collections.unmodifiableList(children));
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error fetching per-replica states", e);
    } catch (InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Thread interrupted when loading per-replica states from " + path, e);
    }
  }


  public static String getReplicaName(String s) {
    int idx = s.indexOf(SEPARATOR);
    if (idx > 0) {
      return s.substring(0, idx);
    }
    return null;
  }

  public State get(String replica) {
    return states.get(replica);
  }

  public static class Operation {
    public final Type typ;
    public final State state;

    public Operation(Type typ, State replicaState) {
      this.typ = typ;
      this.state = replicaState;
    }


    public enum Type {
      //add a new node
      ADD,
      //delete an existing node
      DELETE
    }

    @Override
    public String toString() {
      return typ.toString() + " : " + state;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("{").append(path).append("/[").append(cversion).append("]: [");
    appendStates(sb);
    return sb.append("]}").toString();
  }

  private StringBuilder appendStates(StringBuilder sb) {
    states.forEachEntry(new BiConsumer<String, State>() {
      int count = 0;

      @Override
      public void accept(String s, State state) {
        if (count++ > 0) sb.append(", ");
        sb.append(state.asString);
        for (State d : state.getDuplicates()) sb.append(d.asString);
      }
    });
    return sb;
  }

  /**
   * The state of a replica as stored as a node under /collections/collection-name/state.json/replica-state
   */
  public static class State implements MapWriter {

    public final String replica;

    public final Replica.State state;

    public final Boolean isLeader;

    public final int version;

    public final String asString;

    /**
     * if there are multiple entries for the same replica, e.g: core_node_1:12:A core_node_1:13:D
     * <p>
     * the entry with '13' is the latest and the one with '12' is considered a duplicate
     * <p>
     * These are unlikely, but possible
     */
    final State duplicate;

    private State(String serialized, List<String> pieces) {
      this.asString = serialized;
      replica = pieces.get(0);
      version = Integer.parseInt(pieces.get(1));
      String encodedStatus = pieces.get(2);
      this.state = Replica.getState(encodedStatus);
      isLeader = pieces.size() > 3 && "L".equals(pieces.get(3));
      duplicate = null;
    }

    public static State parse(String serialized) {
      List<String> pieces = StrUtils.splitSmart(serialized, ':');
      if (pieces.size() < 3) return null;
      return new State(serialized, pieces);

    }

    public State(String replica, Replica.State state, Boolean isLeader, int version) {
      this(replica, state, isLeader, version, null);
    }

    public State(String replica, Replica.State state, Boolean isLeader, int version, State duplicate) {
      this.replica = replica;
      this.state = state == null ? Replica.State.ACTIVE : state;
      this.isLeader = isLeader == null ? Boolean.FALSE : isLeader;
      this.version = version;
      asString = serialize();
      this.duplicate = duplicate;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put(NAME, replica);
      ew.put(VERSION, version);
      ew.put(ZkStateReader.STATE_PROP, state.toString());
      if (isLeader) ew.put(Slice.LEADER, isLeader);
      ew.putIfNotNull("duplicate", duplicate);
    }

    private State insert(State duplicate) {
      assert this.replica.equals(duplicate.replica);
      if (this.version >= duplicate.version) {
        if (this.duplicate != null) {
          duplicate = new State(duplicate.replica, duplicate.state, duplicate.isLeader, duplicate.version, this.duplicate);
        }
        return new State(this.replica, this.state, this.isLeader, this.version, duplicate);
      } else {
        return duplicate.insert(this);
      }
    }

    /**
     * fetch duplicates entries for this replica
     */
    List<State> getDuplicates() {
      if (duplicate == null) return Collections.emptyList();
      List<State> result = new ArrayList<>();
      State current = duplicate;
      while (current != null) {
        result.add(current);
        current = current.duplicate;
      }
      return result;
    }

    private String serialize() {
      StringBuilder sb = new StringBuilder(replica)
          .append(":")
          .append(version)
          .append(":")
          .append(state.shortName);
      if (isLeader) sb.append(":").append("L");
      return sb.toString();
    }


    @Override
    public String toString() {
      return asString;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof State) {
        State that = (State) o;
        return Objects.equals(this.asString, that.asString);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return asString.hashCode();
    }

    public State getDuplicate() {
      return duplicate;
    }
  }

}
