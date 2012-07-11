package org.apache.lucene.queryparser.flexible.core.processors;

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

import java.util.*;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * A {@link QueryNodeProcessorPipeline} class should be used to build a query
 * node processor pipeline.
 * 
 * When a query node tree is processed using this class, it passes the query
 * node tree to each processor on the pipeline and the result from each
 * processor is passed to the next one, always following the order the
 * processors were on the pipeline.
 * 
 * When a {@link QueryConfigHandler} object is set on a
 * {@link QueryNodeProcessorPipeline}, it also takes care of setting this
 * {@link QueryConfigHandler} on all processor on pipeline.
 * 
 */
public class QueryNodeProcessorPipeline implements QueryNodeProcessor,
    List<QueryNodeProcessor> {

  private LinkedList<QueryNodeProcessor> processors = new LinkedList<QueryNodeProcessor>();

  private QueryConfigHandler queryConfig;

  /**
   * Constructs an empty query node processor pipeline.
   */
  public QueryNodeProcessorPipeline() {
    // empty constructor
  }

  /**
   * Constructs with a {@link QueryConfigHandler} object.
   */
  public QueryNodeProcessorPipeline(QueryConfigHandler queryConfigHandler) {
    this.queryConfig = queryConfigHandler;
  }

  /**
   * For reference about this method check:
   * {@link QueryNodeProcessor#getQueryConfigHandler()}.
   * 
   * @return QueryConfigHandler the query configuration handler to be set.
   * 
   * @see QueryNodeProcessor#setQueryConfigHandler(QueryConfigHandler)
   * @see QueryConfigHandler
   */
  public QueryConfigHandler getQueryConfigHandler() {
    return this.queryConfig;
  }

  /**
   * For reference about this method check:
   * {@link QueryNodeProcessor#process(QueryNode)}.
   * 
   * @param queryTree the query node tree to be processed
   * 
   * @throws QueryNodeException if something goes wrong during the query node
   *         processing
   * 
   * @see QueryNode
   */
  public QueryNode process(QueryNode queryTree) throws QueryNodeException {

    for (QueryNodeProcessor processor : this.processors) {
      queryTree = processor.process(queryTree);
    }

    return queryTree;

  }

  /**
   * For reference about this method check:
   * {@link QueryNodeProcessor#setQueryConfigHandler(QueryConfigHandler)}.
   * 
   * @param queryConfigHandler the query configuration handler to be set.
   * 
   * @see QueryNodeProcessor#getQueryConfigHandler()
   * @see QueryConfigHandler
   */
  public void setQueryConfigHandler(QueryConfigHandler queryConfigHandler) {
    this.queryConfig = queryConfigHandler;

    for (QueryNodeProcessor processor : this.processors) {
      processor.setQueryConfigHandler(this.queryConfig);
    }

  }

  /**
   * @see List#add(Object)
   */
  public boolean add(QueryNodeProcessor processor) {
    boolean added = this.processors.add(processor);

    if (added) {
      processor.setQueryConfigHandler(this.queryConfig);
    }

    return added;

  }

  /**
   * @see List#add(int, Object)
   */
  public void add(int index, QueryNodeProcessor processor) {
    this.processors.add(index, processor);
    processor.setQueryConfigHandler(this.queryConfig);

  }

  /**
   * @see List#addAll(Collection)
   */
  public boolean addAll(Collection<? extends QueryNodeProcessor> c) {
    boolean anyAdded = this.processors.addAll(c);

    for (QueryNodeProcessor processor : c) {
      processor.setQueryConfigHandler(this.queryConfig);
    }

    return anyAdded;

  }

  /**
   * @see List#addAll(int, Collection)
   */
  public boolean addAll(int index, Collection<? extends QueryNodeProcessor> c) {
    boolean anyAdded = this.processors.addAll(index, c);

    for (QueryNodeProcessor processor : c) {
      processor.setQueryConfigHandler(this.queryConfig);
    }

    return anyAdded;
    
  }

  /**
   * @see List#clear()
   */
  public void clear() {
    this.processors.clear();
  }

  /**
   * @see List#contains(Object)
   */
  public boolean contains(Object o) {
    return this.processors.contains(o);
  }

  /**
   * @see List#containsAll(Collection)
   */
  public boolean containsAll(Collection<?> c) {
    return this.processors.containsAll(c);
  }

  /**
   * @see List#get(int)
   */
  public QueryNodeProcessor get(int index) {
    return this.processors.get(index);
  }

  /**
   * @see List#indexOf(Object)
   */
  public int indexOf(Object o) {
    return this.processors.indexOf(o);
  }

  /**
   * @see List#isEmpty()
   */
  public boolean isEmpty() {
    return this.processors.isEmpty();
  }

  /**
   * @see List#iterator()
   */
  public Iterator<QueryNodeProcessor> iterator() {
    return this.processors.iterator();
  }

  /**
   * @see List#lastIndexOf(Object)
   */
  public int lastIndexOf(Object o) {
    return this.processors.lastIndexOf(o);
  }

  /**
   * @see List#listIterator()
   */
  public ListIterator<QueryNodeProcessor> listIterator() {
    return this.processors.listIterator();
  }

  /**
   * @see List#listIterator(int)
   */
  public ListIterator<QueryNodeProcessor> listIterator(int index) {
    return this.processors.listIterator(index);
  }

  /**
   * @see List#remove(Object)
   */
  public boolean remove(Object o) {
    return this.processors.remove(o);
  }

  /**
   * @see List#remove(int)
   */
  public QueryNodeProcessor remove(int index) {
    return this.processors.remove(index);
  }

  /**
   * @see List#removeAll(Collection)
   */
  public boolean removeAll(Collection<?> c) {
    return this.processors.removeAll(c);
  }

  /**
   * @see List#retainAll(Collection)
   */
  public boolean retainAll(Collection<?> c) {
    return this.processors.retainAll(c);
  }

  /**
   * @see List#set(int, Object)
   */
  public QueryNodeProcessor set(int index, QueryNodeProcessor processor) {
    QueryNodeProcessor oldProcessor = this.processors.set(index, processor);
    
    if (oldProcessor != processor) {
      processor.setQueryConfigHandler(this.queryConfig);
    }
    
    return oldProcessor;
    
  }

  /**
   * @see List#size()
   */
  public int size() {
    return this.processors.size();
  }

  /**
   * @see List#subList(int, int)
   */
  public List<QueryNodeProcessor> subList(int fromIndex, int toIndex) {
    return this.processors.subList(fromIndex, toIndex);
  }

  /**
   * @see List#toArray(Object[])
   */
  public <T> T[] toArray(T[] array) {
    return this.processors.toArray(array);
  }

  /**
   * @see List#toArray()
   */
  public Object[] toArray() {
    return this.processors.toArray();
  }

}
