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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.graph.Traversal.Scatter;

@SuppressWarnings({"rawtypes"})
class TraversalIterator implements Iterator {

  private List<Map<String,Node>> graph;
  private List<String> collections;
  private List<String> fields;

  private Iterator<Iterator<Node>> graphIterator;
  private Iterator<Node> levelIterator;

  private Iterator<String> fieldIterator;
  private Iterator<String> collectionIterator;
  private Iterator<Integer> levelNumIterator;
  private String outField;
  private String outCollection;
  private int outLevel;
  private Traversal traversal;

  public TraversalIterator(Traversal traversal, Set<Scatter> scatter) {
    this.traversal = traversal;
    graph = traversal.getGraph();
    collections = traversal.getCollections();
    fields = traversal.getFields();

    List<String> outCollections = new ArrayList<>();
    List<String> outFields = new ArrayList<>();
    List<Integer> levelNums = new ArrayList<>();
    List<Iterator<Node>> levelIterators = new ArrayList<>();

    if(scatter.contains(Scatter.BRANCHES)) {
      if(graph.size() > 1) {
        for(int i=0; i<graph.size()-1; i++) {
          Map<String, Node> graphLevel = graph.get(i);
          String collection = collections.get(i);
          String field = fields.get(i);
          outCollections.add(collection);
          outFields.add(field);
          levelNums.add(i);
          levelIterators.add(graphLevel.values().iterator());
        }
      }
    }

    if(scatter.contains(Scatter.LEAVES)) {
      int leavesLevel = graph.size() > 1 ? graph.size()-1 : 0 ;
      Map<String, Node> graphLevel = graph.get(leavesLevel);
      String collection = collections.get(leavesLevel);
      String field = fields.get(leavesLevel);
      levelNums.add(leavesLevel);
      outCollections.add(collection);
      outFields.add(field);
      levelIterators.add(graphLevel.values().iterator());
    }

    graphIterator = levelIterators.iterator();
    levelIterator = graphIterator.next();

    fieldIterator = outFields.iterator();
    collectionIterator = outCollections.iterator();
    levelNumIterator = levelNums.iterator();

    outField = fieldIterator.next();
    outCollection = collectionIterator.next();
    outLevel = levelNumIterator.next();
  }

  @Override
  public boolean hasNext() {
    if(levelIterator.hasNext()) {
      return true;
    } else {
      if(graphIterator.hasNext()) {
        levelIterator = graphIterator.next();
        outField = fieldIterator.next();
        outCollection = collectionIterator.next();
        outLevel = levelNumIterator.next();
        return hasNext();
      } else {
        return false;
      }
    }
  }

  @Override
  public Tuple next() {
    if(hasNext()) {
      Node node = levelIterator.next();
      return node.toTuple(outCollection, outField, outLevel, traversal);
    } else {
     return null;
    }
  }
}