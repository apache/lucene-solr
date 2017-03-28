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

package org.apache.solr.recipe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.util.Utils;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class Policy {
  public static final String EACH = "#EACH";
  public static final String ANY = "#ANY";
  List<Clause> clauses = new ArrayList<>();
  List<Preference> preferences = new ArrayList<>();
  List<String> params= new ArrayList<>();


  public Policy(Map<String, Object> jsonMap) {
    List<Map<String, Object>> l = getListOfMap("conditions", jsonMap);
    clauses = l.stream().map(Clause::new).collect(toList());
    l = getListOfMap("preferences", jsonMap);
    preferences = l.stream().map(Preference::new).collect(toList());
    for (int i = 0; i < preferences.size() - 1; i++) {
      Preference preference = preferences.get(i);
      preference.next = preferences.get(i + 1);
    }

    for (Clause c : clauses) params.add(c.tag.name);
    for (Preference preference : preferences) {
      if (params.contains(preference.name.name())) {
        throw new RuntimeException(preference.name + " is repeated");
      }
      params.add(preference.name.toString());
      preference.idx = params.size() - 1;
    }
  }


  public class Session implements MapWriter {
    final List<String> nodes;
    final NodeValueProvider snitch;
    final List<Row> matrix;
    Set<String> collections = new HashSet<>();

    Session(List<String> nodes, NodeValueProvider snitch) {
      this.nodes = nodes;
      this.snitch = snitch;
      matrix = new ArrayList<>(nodes.size());
      for (String node : nodes) matrix.add(new Row(node, params, snitch));
      for (Row row : matrix) row.replicaInfo.forEach((s, e) -> collections.add(s));
    }

    List<Row> getMatrixCopy() {
      return matrix.stream()
          .map(Row::copy)
          .collect(Collectors.toList());
    }

    Policy getRuleSorter() {
      return Policy.this;

    }

    /**Apply the preferences and conditions
     */
    public void applyRules() {
      if (!preferences.isEmpty()) {
        //this is to set the approximate value according to the precision
        ArrayList<Row> tmpMatrix = new ArrayList<>(matrix);
        for (Preference p : preferences) {
          Collections.sort(tmpMatrix, (r1, r2) -> p.compare(r1, r2, false));
          p.setApproxVal(tmpMatrix);
        }
        //approximate values are set now. Let's do recursive sorting
        Collections.sort(matrix, (r1, r2) -> preferences.get(0).compare(r1, r2, true));
      }

      if (!clauses.isEmpty()) {
        for (Clause clause : clauses) {
          for (Row row : matrix) {
            clause.test(row);
          }
        }
      }

    }

    public Map<String, List<Clause>> getViolations() {
      return matrix.stream()
          .filter(row -> !row.violations.isEmpty())
          .collect(Collectors.toMap(r -> r.node, r -> r.violations));
    }

    public Map suggest(CollectionAction action, String collection, String shard) {
      Suggester op = ops.get(action);
      if (op == null) throw new UnsupportedOperationException(action.toString() + "is not supported");
      return op.suggest(collection, shard, this);
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      for (int i = 0; i < matrix.size(); i++) {
        Row row = matrix.get(i);
        ew.put(row.node, row);
      }
    }

    @Override
    public String toString() {
      return Utils.toJSONString(toMap(new LinkedHashMap<>()));
    }
    public List<Row> getSorted(){
      return Collections.unmodifiableList(matrix);
    }
  }


  public Session createSession(List<String> nodes, NodeValueProvider snitch) {
    return new Session(nodes, snitch);
  }


  static List<Map<String, Object>> getListOfMap(String key, Map<String, Object> jsonMap) {
    Object o = jsonMap.get(key);
    if (o != null) {
      if (!(o instanceof List)) o = singletonList(o);
      return (List) o;
    } else {
      return Collections.emptyList();
    }
  }


  enum SortParam {
    freedisk, cores, heap, cpu;

    static SortParam get(String m) {
      for (SortParam p : values()) if (p.name().equals(m)) return p;
      throw new RuntimeException("Sort must be on one of these " + Arrays.asList(values()));
    }

  }

  enum Sort {
    maximize(1), minimize(-1);
    final int sortval;

    Sort(int i) {
      sortval = i;
    }

    static Sort get(Map<String, Object> m) {
      if (m.containsKey(maximize.name()) && m.containsKey(minimize.name())) {
        throw new RuntimeException("Cannot have both 'maximize' and 'minimize'");
      }
      if (m.containsKey(maximize.name())) return maximize;
      if (m.containsKey(minimize.name())) return minimize;
      throw new RuntimeException("must have either 'maximize' or 'minimize'");
    }
  }


  static class ReplicaStat implements MapWriter {
    final String name;
    Map<String, Object> variables;

    ReplicaStat(String name, Map<String, Object> vals) {
      this.name = name;
      this.variables = vals;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put(name, variables);
    }
  }


  interface NodeValueProvider {
    Map<String, Object> getValues(String node, Collection<String> keys);

    /**
     * Get the details of each replica in a node. It attempts to fetch as much details about
     * the replica as mentioned in the keys list
     * <p>
     * the format is {collection:shard :[{replicadetails}]}
     */
    Map<String, Map<String, List<ReplicaStat>>> getReplicaCounts(String node, Collection<String> keys);
  }

  interface Suggester {
    Map<String, Object> suggest(String coll, String shard, Session session);

  }

  static class BaseSuggester {
    final String coll;
    final String shard;
    final Policy.Session session;
    List<Row> matrix;

    BaseSuggester(String coll, String shard, Policy.Session session) {
      this.coll = coll;
      this.shard = shard;
      this.session = session;
      matrix = session.getMatrixCopy();
    }


  }

  private static final Map<CollectionAction, Suggester> ops = new HashMap<>();

  static {
    ops.put(CollectionAction.ADDREPLICA, (coll, shard, session) -> new AddReplicaSuggester(coll, shard, session).get());
    ops.put(CollectionAction.MOVEREPLICA, (coll, shard, session) -> new MoveReplicaSuggester(coll, shard, session).get());
  }



}
