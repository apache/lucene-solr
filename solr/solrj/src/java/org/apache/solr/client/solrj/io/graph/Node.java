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

package org.apache.solr.client.solrj.io.graph;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.metrics.*;
import java.util.*;

public class Node {

  private String id;
  private List<Metric> metrics;
  private Set<String> ancestors;

  public Node(String id, boolean track) {
    this.id=id;
    if(track) {
      ancestors = new TreeSet<>();
    }
  }

  public void setMetrics(List<Metric> metrics) {
    this.metrics = metrics;
  }

  public void add(String ancestor, Tuple tuple) {
    if(ancestors != null) {
      ancestors.add(ancestor);
    }

    if(metrics != null) {
      for(Metric metric : metrics) {
        metric.update(tuple);
      }
    }
  }

  public Tuple toTuple(String collection, String field, int level, Traversal traversal) {
    Tuple tuple = new Tuple();

    tuple.put("node", id);
    tuple.put("collection", collection);
    tuple.put("field", field);
    tuple.put("level", level);

    boolean prependCollection = traversal.isMultiCollection();
    List<String> cols = traversal.getCollections();

    if(ancestors != null) {
      List<String> l = new ArrayList<>();
      for(String ancestor : ancestors) {
        String[] ancestorParts = ancestor.split("\\^");

        if(prependCollection) {
          //prepend the collection
          int colIndex = Integer.parseInt(ancestorParts[0]);
          l.add(cols.get(colIndex)+"/"+ancestorParts[1]);
        } else {
          // Use only the ancestor id.
          l.add(ancestorParts[1]);
        }
      }

      tuple.put("ancestors", l);
    }

    if(metrics != null) {
      for(Metric metric : metrics) {
        tuple.put(metric.getIdentifier(), metric.getValue());
      }
    }

    return tuple;
  }
}