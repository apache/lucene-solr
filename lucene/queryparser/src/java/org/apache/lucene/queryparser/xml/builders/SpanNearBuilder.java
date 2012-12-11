package org.apache.lucene.queryparser.xml.builders;

import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
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
 * Builder for {@link SpanNearQuery}
 */
public class SpanNearBuilder extends SpanBuilderBase {

  private final SpanQueryBuilder factory;

  public SpanNearBuilder(SpanQueryBuilder factory) {
    this.factory = factory;
  }

  @Override
  public SpanQuery getSpanQuery(Element e) throws ParserException {
    String slopString = DOMUtils.getAttributeOrFail(e, "slop");
    int slop = Integer.parseInt(slopString);
    boolean inOrder = DOMUtils.getAttribute(e, "inOrder", false);
    List<SpanQuery> spans = new ArrayList<SpanQuery>();
    for (Node kid = e.getFirstChild(); kid != null; kid = kid.getNextSibling()) {
      if (kid.getNodeType() == Node.ELEMENT_NODE) {
        spans.add(factory.getSpanQuery((Element) kid));
      }
    }
    SpanQuery[] spanQueries = spans.toArray(new SpanQuery[spans.size()]);
    return new SpanNearQuery(spanQueries, slop, inOrder);
  }

}
