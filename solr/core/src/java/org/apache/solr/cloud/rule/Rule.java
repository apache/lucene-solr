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
package org.apache.solr.cloud.rule;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.cloud.rule.Rule.MatchStatus.CANNOT_ASSIGN_FAIL;
import static org.apache.solr.cloud.rule.Rule.MatchStatus.NODE_CAN_BE_ASSIGNED;
import static org.apache.solr.cloud.rule.Rule.MatchStatus.NOT_APPLICABLE;
import static org.apache.solr.cloud.rule.Rule.Operand.EQUAL;
import static org.apache.solr.cloud.rule.Rule.Operand.GREATER_THAN;
import static org.apache.solr.cloud.rule.Rule.Operand.LESS_THAN;
import static org.apache.solr.cloud.rule.Rule.Operand.NOT_EQUAL;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.rule.ImplicitSnitch.CORES;

/** @deprecated to be removed in Solr 9.0 (see SOLR-14930)
 *
 */
public class Rule {
  public static final String WILD_CARD = "*";
  public static final String WILD_WILD_CARD = "**";
  static final Condition SHARD_DEFAULT = new Rule.Condition(SHARD_ID_PROP, WILD_WILD_CARD);
  static final Condition REPLICA_DEFAULT = new Rule.Condition(REPLICA_PROP, WILD_CARD);
  Condition shard;
  Condition replica;
  Condition tag;

  public Rule(@SuppressWarnings({"rawtypes"})Map m) {
    for (Object o : m.entrySet()) {
      @SuppressWarnings({"rawtypes"})
      Map.Entry e = (Map.Entry) o;
      Condition condition = new Condition(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
      if (condition.name.equals(SHARD_ID_PROP)) shard = condition;
      else if (condition.name.equals(REPLICA_PROP)) replica = condition;
      else {
        if (tag != null) {
          throw new RuntimeException("There can be only one and only one tag other than 'shard' and 'replica' in rule " + m);
        }
        tag = condition;
      }

    }
    if (shard == null) shard = SHARD_DEFAULT;
    if (replica == null) replica = REPLICA_DEFAULT;
    if (tag == null) throw new RuntimeException("There should be a tag other than 'shard' and 'replica'");
    if (replica.isWildCard() && tag.isWildCard()) {
      throw new RuntimeException("Both replica and tag cannot be wild cards");
    }

  }

  static Object parseObj(Object o, @SuppressWarnings({"rawtypes"})Class typ) {
    if (o == null) return o;
    if (typ == String.class) return String.valueOf(o);
    if (typ == Integer.class) {
      Double v = Double.parseDouble(String.valueOf(o));
      return v.intValue();
    }
    return o;
  }

  @SuppressWarnings({"rawtypes"})
  public static Map parseRule(String s) {
    Map<String, String> result = new LinkedHashMap<>();
    s = s.trim();
    List<String> keyVals = StrUtils.splitSmart(s, ',');
    for (String kv : keyVals) {
      List<String> keyVal = StrUtils.splitSmart(kv, ':');
      if (keyVal.size() != 2) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid rule. should have only key and val in : " + kv);
      }
      if (keyVal.get(0).trim().length() == 0 || keyVal.get(1).trim().length() == 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid rule. should have key and val in : " + kv);
      }
      result.put(keyVal.get(0).trim(), keyVal.get(1).trim());
    }
    return result;
  }


  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public String toString() {
    @SuppressWarnings({"rawtypes"})
    Map map = new LinkedHashMap();
    if (shard != SHARD_DEFAULT) map.put(shard.name, shard.operand.toStr(shard.val));
    if (replica != REPLICA_DEFAULT) map.put(replica.name, replica.operand.toStr(replica.val));
    map.put(tag.name, tag.operand.toStr(tag.val));
    return Utils.toJSONString(map);
  }

  /**
   * Check if it is possible to assign this node as a replica of the given shard
   * without violating this rule
   *
   * @param testNode       The node in question
   * @param shardVsNodeSet Set of nodes for every shard 
   * @param nodeVsTags     The pre-fetched tags for all the nodes
   * @param shardName      The shard to which this node should be attempted
   * @return MatchStatus
   */
  MatchStatus tryAssignNodeToShard(String testNode,
                                   Map<String, Map<String,Integer>> shardVsNodeSet,
                                   Map<String, Map<String, Object>> nodeVsTags,
                                   String shardName, Phase phase) {

    if (tag.isWildCard()) {
      //this is ensuring uniqueness across a certain tag
      //eg: rack:r168
      if (!shard.isWildCard() && shardName.equals(shard.val)) return NOT_APPLICABLE;
      Object tagValueForThisNode = nodeVsTags.get(testNode).get(tag.name);
      int v = getNumberOfNodesWithSameTagVal(shard, nodeVsTags, shardVsNodeSet,
          shardName, new Condition(tag.name, tagValueForThisNode, EQUAL), phase);
      if (phase == Phase.ASSIGN || phase == Phase.FUZZY_ASSIGN)
        v++;//v++ because including this node , it becomes v+1 during ASSIGN
      return replica.canMatch(v, phase) ?
          NODE_CAN_BE_ASSIGNED :
          CANNOT_ASSIGN_FAIL;
    } else {
      if (!shard.isWildCard() && !shardName.equals(shard.val)) return NOT_APPLICABLE;
      if (replica.isWildCard()) {
        //this means for each replica, the value must match
        //shard match is already tested
        Map<String, Object> tags = nodeVsTags.get(testNode);
        if (tag.canMatch(tags == null ? null : tags.get(tag.name), phase)) return NODE_CAN_BE_ASSIGNED;
        else return CANNOT_ASSIGN_FAIL;
      } else {
        int v = getNumberOfNodesWithSameTagVal(shard, nodeVsTags, shardVsNodeSet, shardName, tag, phase);
        return replica.canMatch(v, phase) ? NODE_CAN_BE_ASSIGNED : CANNOT_ASSIGN_FAIL;

      }

    }
  }

  private int getNumberOfNodesWithSameTagVal(Condition shardCondition,
                                             Map<String, Map<String, Object>> nodeVsTags,
                                             Map<String, Map<String,Integer>> shardVsNodeSet,
                                             String shardName,
                                             Condition tagCondition,
                                             Phase phase) {

    int countMatchingThisTagValue = 0;
    for (Map.Entry<String, Map<String,Integer>> entry : shardVsNodeSet.entrySet()) {
      //check if this shard is relevant. either it is a ANY Wild card (**)
      // or this shard is same as the shard in question
      if (shardCondition.val.equals(WILD_WILD_CARD) || entry.getKey().equals(shardName)) {
        Map<String,Integer> nodesInThisShard = shardVsNodeSet.get(shardCondition.val.equals(WILD_WILD_CARD) ? entry.getKey() : shardName);
        if (nodesInThisShard != null) {
          for (Map.Entry<String,Integer> aNode : nodesInThisShard.entrySet()) {
            Map<String, Object> tagValues = nodeVsTags.get(aNode.getKey());
            if(tagValues == null) continue;
            Object obj = tagValues.get(tag.name);
            if (tagCondition.canMatch(obj, phase)) countMatchingThisTagValue += aNode.getValue();
          }
        }
      }
    }
    return countMatchingThisTagValue;
  }

  public int compare(String n1, String n2,
                     Map<String, Map<String, Object>> nodeVsTags,
                     Map<String, Map<String,Integer>> currentState) {
    return tag.compare(n1, n2, nodeVsTags);
  }

  public boolean isFuzzy() {
    return shard.fuzzy || replica.fuzzy || tag.fuzzy;
  }

  public enum Operand {
    EQUAL(""),
    NOT_EQUAL("!") {
      @Override
      public boolean canMatch(Object ruleVal, Object testVal) {
        return !super.canMatch(ruleVal, testVal);
      }
    },
    GREATER_THAN(">") {
      @Override
      public Object match(String val) {
        return checkNumeric(super.match(val));
      }


      @Override
      public boolean canMatch(Object ruleVal, Object testVal) {
        return testVal != null && compareNum(ruleVal, testVal) == 1;
      }

    },
    LESS_THAN("<") {
      @Override
      public int compare(Object n1Val, Object n2Val) {
        return GREATER_THAN.compare(n1Val, n2Val) * -1;
      }

      @Override
      public boolean canMatch(Object ruleVal, Object testVal) {
        return testVal != null && compareNum(ruleVal, testVal) == -1;
      }

      @Override
      public Object match(String val) {
        return checkNumeric(super.match(val));
      }
    };
    public final String operand;

    Operand(String val) {
      this.operand = val;
    }

    public String toStr(Object expectedVal) {
      return operand + expectedVal.toString();
    }

    Object checkNumeric(Object val) {
      if (val == null) return null;
      try {
        return Integer.parseInt(val.toString());
      } catch (NumberFormatException e) {
        throw new RuntimeException("for operand " + operand + " the value must be numeric");
      }
    }

    public Object match(String val) {
      if (operand.isEmpty()) return val;
      return val.startsWith(operand) ? val.substring(1) : null;
    }

    public boolean canMatch(Object ruleVal, Object testVal) {
      return Objects.equals(String.valueOf(ruleVal), String.valueOf(testVal));
    }


    public int compare(Object n1Val, Object n2Val) {
      return 0;
    }

    public int compareNum(Object n1Val, Object n2Val) {
      Integer n1 = (Integer) parseObj(n1Val, Integer.class);
      Integer n2 = (Integer) parseObj(n2Val, Integer.class);
      return n1 > n2 ? -1 : Objects.equals(n1, n2) ? 0 : 1;
    }
  }

  enum MatchStatus {
    NODE_CAN_BE_ASSIGNED,
    CANNOT_ASSIGN_GO_AHEAD,
    NOT_APPLICABLE,
    CANNOT_ASSIGN_FAIL
  }

  enum Phase {
    ASSIGN, VERIFY, FUZZY_ASSIGN, FUZZY_VERIFY
  }

  public static class Condition {
    public final String name;
    final Object val;
    public final Operand operand;
    final boolean fuzzy;

    Condition(String name, Object val, Operand op) {
      this.name = name;
      this.val = val;
      this.operand = op;
      fuzzy = false;
    }

    Condition(String key, Object val) {
      Object expectedVal;
      boolean fuzzy = false;
      if (val == null) throw new RuntimeException("value of  a tag cannot be null for key " + key);
      try {
        this.name = key.trim();
        String value = val.toString().trim();
        if (value.endsWith("~")) {
          fuzzy = true;
          value = value.substring(0, value.length() - 1);
        }
        if ((expectedVal = NOT_EQUAL.match(value)) != null) {
          operand = NOT_EQUAL;
        } else if ((expectedVal = GREATER_THAN.match(value)) != null) {
          operand = GREATER_THAN;
        } else if ((expectedVal = LESS_THAN.match(value)) != null) {
          operand = LESS_THAN;
        } else {
          operand = EQUAL;
          expectedVal = value;
        }

        if (name.equals(REPLICA_PROP)) {
          if (!WILD_CARD.equals(expectedVal)) {
            try {
              expectedVal = Integer.parseInt(expectedVal.toString());
            } catch (NumberFormatException e) {
              throw new RuntimeException("The replica tag value can only be '*' or an integer");
            }
          }
        }

      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid condition : " + key + ":" + val, e);
      }
      this.val = expectedVal;
      this.fuzzy = fuzzy;

    }

    public boolean isWildCard() {
      return val.equals(WILD_CARD) || val.equals(WILD_WILD_CARD);
    }

    boolean canMatch(Object testVal, Phase phase) {
      if (phase == Phase.FUZZY_ASSIGN || phase == Phase.FUZZY_VERIFY) return true;
      if (phase == Phase.ASSIGN) {
        if ((name.equals(REPLICA_PROP) || name.equals(CORES)) &&
            (operand == GREATER_THAN || operand == NOT_EQUAL)) {
          //the no:of replicas or cores will increase towards the end
          //so this should only be checked in the Phase.
          //process
          return true;
        }
      }

      return operand.canMatch(val, testVal);
    }


    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Condition) {
        Condition that = (Condition) obj;
        return Objects.equals(name, that.name) &&
            Objects.equals(operand, that.operand) &&
            Objects.equals(val, that.val);

      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, operand);
    }

    @Override
    public String toString() {
      return name + ":" + operand.toStr(val) + (fuzzy ? "~" : "");
    }

    public Integer getInt() {
      return (Integer) val;
    }

    public int compare(String n1, String n2, Map<String, Map<String, Object>> nodeVsTags) {
      Map<String, Object> tags = nodeVsTags.get(n1);
      Object n1Val = tags == null ? null : tags.get(name);
      tags = nodeVsTags.get(n2);
      Object n2Val = tags == null ? null : tags.get(name);
      if (n1Val == null || n2Val == null) return -1;
      return isWildCard() ? 0 : operand.compare(n1Val, n2Val);
    }

  }


}



