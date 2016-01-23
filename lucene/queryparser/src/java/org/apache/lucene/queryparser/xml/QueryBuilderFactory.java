/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.queryparser.xml;

import org.apache.lucene.search.Query;
import org.w3c.dom.Element;

import java.util.HashMap;
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

/**
 * Factory for {@link QueryBuilder}
 */
public class QueryBuilderFactory implements QueryBuilder {

  HashMap<String, QueryBuilder> builders = new HashMap<>();

  @Override
  public Query getQuery(Element n) throws ParserException {
    QueryBuilder builder = builders.get(n.getNodeName());
    if (builder == null) {
      throw new ParserException("No QueryObjectBuilder defined for node " + n.getNodeName());
    }
    return builder.getQuery(n);
  }

  public void addBuilder(String nodeName, QueryBuilder builder) {
    builders.put(nodeName, builder);
  }

  public QueryBuilder getQueryBuilder(String nodeName) {
    return builders.get(nodeName);
  }

}
