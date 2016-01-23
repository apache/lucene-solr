package org.apache.lucene.queryParser.core.processors;

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

import java.util.LinkedList;

import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.core.nodes.QueryNode;

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
 * {@link QueryNodeProcessorPipeline}, it takes care of also setting this
 * {@link QueryConfigHandler} on all processor on pipeline.
 * 
 */
public class QueryNodeProcessorPipeline implements QueryNodeProcessor {

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
   * @param queryTree
   *          the query node tree to be processed
   * 
   * @throws QueryNodeException
   *           if something goes wrong during the query node processing
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
   * Adds a processor to the pipeline, it's always added to the end of the
   * pipeline.
   * 
   * @param processor
   *          the processor to be added
   */
  public void addProcessor(QueryNodeProcessor processor) {
    this.processors.add(processor);

    processor.setQueryConfigHandler(this.queryConfig);

  }

  /**
   * For reference about this method check:
   * {@link QueryNodeProcessor#setQueryConfigHandler(QueryConfigHandler)}.
   * 
   * @param queryConfigHandler
   *          the query configuration handler to be set.
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

}
