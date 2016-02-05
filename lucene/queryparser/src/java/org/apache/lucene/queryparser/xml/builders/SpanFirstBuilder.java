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

import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.w3c.dom.Element;
/**
 * Builder for {@link SpanFirstQuery}
 */
public class SpanFirstBuilder extends SpanBuilderBase {

  private final SpanQueryBuilder factory;

  public SpanFirstBuilder(SpanQueryBuilder factory) {
    this.factory = factory;
  }

  @Override
  public SpanQuery getSpanQuery(Element e) throws ParserException {
    int end = DOMUtils.getAttribute(e, "end", 1);
    Element child = DOMUtils.getFirstChildElement(e);
    SpanQuery q = factory.getSpanQuery(child);

    SpanFirstQuery sfq = new SpanFirstQuery(q, end);

    float boost = DOMUtils.getAttribute(e, "boost", 1.0f);
    return new SpanBoostQuery(sfq, boost);
  }

}
