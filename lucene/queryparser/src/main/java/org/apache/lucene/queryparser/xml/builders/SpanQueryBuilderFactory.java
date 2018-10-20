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
package org.apache.lucene.queryparser.xml.builders;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.queryparser.xml.ParserException;
import org.w3c.dom.Element;

import java.util.HashMap;
import java.util.Map;
/**
 * Factory for {@link SpanQueryBuilder}s
 */
public class SpanQueryBuilderFactory implements SpanQueryBuilder {

  private final Map<String, SpanQueryBuilder> builders = new HashMap<>();

  @Override
  public Query getQuery(Element e) throws ParserException {
    return getSpanQuery(e);
  }

  public void addBuilder(String nodeName, SpanQueryBuilder builder) {
    builders.put(nodeName, builder);
  }

  @Override
  public SpanQuery getSpanQuery(Element e) throws ParserException {
    SpanQueryBuilder builder = builders.get(e.getNodeName());
    if (builder == null) {
      throw new ParserException("No SpanQueryObjectBuilder defined for node " + e.getNodeName());
    }
    return builder.getSpanQuery(e);
  }

}
