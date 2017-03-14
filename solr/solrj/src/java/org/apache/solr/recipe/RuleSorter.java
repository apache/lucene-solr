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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.recipe.Operand.GREATER_THAN;
import static org.apache.solr.recipe.Operand.LESS_THAN;
import static org.apache.solr.recipe.Operand.NOT_EQUAL;

public class RuleSorter {
  public static final String ALL = "#ALL";
  public static final String EACH = "#EACH";
  List<Clause> conditionClauses = new ArrayList<>();
  List<Preference> preferences = new ArrayList<>();
  List<String> params= new ArrayList<>();


  public RuleSorter(Map<String, Object> jsonMap) {
    List<Map<String, Object>> l = getListOfMap("conditions", jsonMap);
    conditionClauses = l.stream().map(Clause::new).collect(toList());
    l = getListOfMap("preferences", jsonMap);
    preferences = l.stream().map(Preference::new).collect(toList());
    for (int i = 0; i < preferences.size() - 1; i++) {
      Preference preference = preferences.get(i);
      preference.next = preferences.get(i + 1);
    }

    for (Clause c : conditionClauses) {
      for (Condition condition : c.conditions) params.add(condition.name);
    }

    for (Preference preference : preferences) {
      if (params.contains(preference.name.name())) {
        throw new RuntimeException(preference.name + " is repeated");
      }
      params.add(preference.name.toString());
      preference.idx = params.size() - 1;
    }

  }

  public class Session implements MapWriter {
    private final List<String> nodes;
    private final NodeValueProvider snitch;
    private List<Row> matrix;
    List<String> paramsList = new ArrayList<>(params);

    private Session(List<String> nodes, NodeValueProvider snitch) {
      this.nodes = nodes;
      this.snitch = snitch;
      matrix = new ArrayList<>(nodes.size());
      for (String node : nodes) matrix.add(new Row(node, paramsList, snitch));

    }

    public void sort() {
      if (preferences.size() > 1) {
        //this is to set the approximate value according to the precision
        ArrayList<Row> tmpMatrix = new ArrayList<>(matrix);
        for (Preference p : preferences) {
          Collections.sort(tmpMatrix, (r1, r2) -> p.compare(r1, r2, false));
          p.setNewVal(tmpMatrix);
        }
        //approximate values are set now. Let's do recursive sorting
        Collections.sort(matrix, (r1, r2) -> preferences.get(0).compare(r1, r2, true));
      }
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


  private static List<Map<String, Object>> getListOfMap(String key, Map<String, Object> jsonMap) {
    Object o = jsonMap.get(key);
    if (o != null) {
      if (!(o instanceof List)) o = singletonList(o);
      return (List) o;
    } else {
      return Collections.emptyList();
    }
  }


  static class Clause {
    List<Condition> conditions;
    boolean strict = true;

    Clause(Map<String, Object> m) {
      conditions = m.entrySet().stream()
          .filter(e -> !"strict".equals(e.getKey().trim()))
          .map(Condition::new)
          .collect(toList());
      Object o = m.get("strict");
      if (o == null) return;
      strict = o instanceof Boolean ? (Boolean) o : Boolean.parseBoolean(o.toString());
    }

  }

  static class Condition {
    String name;
    Object val;
    Operand operand;

    Condition(Map.Entry<String, Object> m) {
      Object expectedVal;
      try {
        this.name = m.getKey().trim();
        String value = m.getValue().toString().trim();
        if ((expectedVal = NOT_EQUAL.match(value)) != null) {
          operand = NOT_EQUAL;
        } else if ((expectedVal = GREATER_THAN.match(value)) != null) {
          operand = GREATER_THAN;
        } else if ((expectedVal = LESS_THAN.match(value)) != null) {
          operand = LESS_THAN;
        } else {
          operand = Operand.EQUAL;
          expectedVal = value;
        }

        if (name.equals(REPLICA_PROP)) {
          if (!ALL.equals(expectedVal)) {
            try {
              expectedVal = Integer.parseInt(expectedVal.toString());
            } catch (NumberFormatException e) {
              throw new RuntimeException("The replica tag value can only be '*' or an integer");
            }
          }
        }

      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid condition : " + name + ":" + val, e);
      }
      this.val = expectedVal;


    }
  }

  static class Preference {
    final SortParam name;
    Integer precision;
    final Sort sort;
    Preference next;
    public int idx;

    Preference(Map<String, Object> m) {
      sort = Sort.get(m);
      name = SortParam.get(m.get(sort.name()).toString());
      Object p = m.getOrDefault("precision", 0);
      precision = p instanceof Number ? ((Number) p).intValue() : Integer.parseInt(p.toString());

    }

    // there are 2 modes of compare.
    // recursive, it uses the precision to tie & when there is a tie use the next preference to compare
    // in non-recursive mode, precision is not taken into consideration and sort is done on actual value
    int compare(Row r1, Row r2, boolean recursive) {
      Object o1 = recursive ? r1.cells[idx].val_ : r1.cells[idx].val;
      Object o2 = recursive ? r2.cells[idx].val_ : r2.cells[idx].val;
      int result = 0;
      if (o1 instanceof Integer && o2 instanceof Integer) result = ((Integer) o1).compareTo((Integer) o2);
      if (o1 instanceof Long && o2 instanceof Long) result = ((Long) o1).compareTo((Long) o2);
      if (o1 instanceof Float && o2 instanceof Float) result = ((Float) o1).compareTo((Float) o2);
      if (o1 instanceof Double && o2 instanceof Double) result = ((Double) o1).compareTo((Double) o2);
      return result == 0 ? next == null ? 0 : next.compare(r1, r2, recursive) : sort.sortval * result;
    }

    //sets the new value according to precision in val_
    void setNewVal(List<Row> tmpMatrix) {
      Object prevVal = null;
      for (Row row : tmpMatrix) {
        prevVal = row.cells[idx].val_ =
            prevVal == null || Math.abs(((Number) prevVal).longValue() - ((Number) row.cells[idx].val).longValue()) > precision ?
                row.cells[idx].val :
                prevVal;
      }
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

    public static Sort get(Map<String, Object> m) {
      if (m.containsKey(maximize.name()) && m.containsKey(minimize.name())) {
        throw new RuntimeException("Cannot have both 'maximize' and 'minimize'");
      }
      if (m.containsKey(maximize.name())) return maximize;
      if (m.containsKey(minimize.name())) return minimize;
      throw new RuntimeException("must have either 'maximize' or 'minimize'");
    }
  }

  public static class Row implements MapWriter {
    public final String node;
    final Cell[] cells;
    boolean anyValueMissing = false;

    Row(String node, List<String> params, NodeValueProvider snitch) {
      this.node = node;
      cells = new Cell[params.size()];
      Map<String, Object> vals = new HashMap<>();
      for (String param : params) vals.put(param, null);
      snitch.getValues(node, vals);
      for (int i = 0; i < params.size(); i++) {
        String s = params.get(i);
        cells[i] = new Cell(i, s, vals.get(s));
        if (cells[i].val == null) anyValueMissing = true;
      }
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put(node, (IteratorWriter) iw -> {
        for (Cell cell : cells) iw.add(cell);
      });
    }
  }

  static class Cell implements MapWriter {
    final int index;
    final String name;
    Object val, val_;

    Cell(int index, String name, Object val) {
      this.index = index;
      this.name = name;
      this.val = val;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put(name, val);
    }
  }

  interface NodeValueProvider {

    void getValues(String node, Map<String, Object> valuesMap);

  }
}
