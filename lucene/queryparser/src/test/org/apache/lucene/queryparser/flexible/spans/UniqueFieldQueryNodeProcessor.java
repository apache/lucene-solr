package org.apache.lucene.queryparser.flexible.spans;

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

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldableNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;

/**
 * This processor changes every field name of each {@link FieldableNode} query
 * node contained in the query tree to the field name defined in the
 * {@link UniqueFieldAttribute}. So, the {@link UniqueFieldAttribute} must be
 * defined in the {@link QueryConfigHandler} object set in this processor,
 * otherwise it throws an exception.<br/>
 * <br/>
 * 
 * @see UniqueFieldAttribute
 */
public class UniqueFieldQueryNodeProcessor extends QueryNodeProcessorImpl {

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    return node;

  }

  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {

    if (node instanceof FieldableNode) {
      FieldableNode fieldNode = (FieldableNode) node;

      QueryConfigHandler queryConfig = getQueryConfigHandler();

      if (queryConfig == null) {
        throw new IllegalArgumentException(
            "A config handler is expected by the processor UniqueFieldQueryNodeProcessor!");
      }

      if (!queryConfig.has(SpansQueryConfigHandler.UNIQUE_FIELD)) {
        throw new IllegalArgumentException(
            "UniqueFieldAttribute should be defined in the config handler!");
      }

      String uniqueField = queryConfig.get(SpansQueryConfigHandler.UNIQUE_FIELD);
      fieldNode.setField(uniqueField);

    }

    return node;

  }

  @Override
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children)
      throws QueryNodeException {

    return children;

  }

}
