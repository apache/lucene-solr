package org.apache.lucene.queryParser.original.nodes;

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

import java.text.Collator;

import org.apache.lucene.queryParser.core.nodes.ParametricQueryNode;
import org.apache.lucene.queryParser.core.nodes.ParametricRangeQueryNode;
import org.apache.lucene.queryParser.original.config.RangeCollatorAttribute;
import org.apache.lucene.queryParser.original.processors.ParametricRangeQueryNodeProcessor;
import org.apache.lucene.search.MultiTermQuery;

/**
 * This query node represents a range query. It also holds which collator will
 * be used by the range query and if the constant score rewrite is enabled. <br/>
 * 
 * @see ParametricRangeQueryNodeProcessor
 * @see RangeCollatorAttribute
 * @see org.apache.lucene.search.RangeQuery
 */
public class RangeQueryNode extends ParametricRangeQueryNode {

  private static final long serialVersionUID = 7400866652044314657L;

  private Collator collator;

  private MultiTermQuery.RewriteMethod multiTermRewriteMethod;

  /**
   * @param lower
   * @param upper
   */
  public RangeQueryNode(ParametricQueryNode lower, ParametricQueryNode upper,
      Collator collator, MultiTermQuery.RewriteMethod multiTermRewriteMethod) {
    super(lower, upper);

    this.multiTermRewriteMethod = multiTermRewriteMethod;
    this.collator = collator;

  }

  public String toString() {
    StringBuilder sb = new StringBuilder("<range>\n\t");
    sb.append(this.getUpperBound()).append("\n\t");
    sb.append(this.getLowerBound()).append("\n");
    sb.append("</range>\n");

    return sb.toString();

  }

  /**
   * @return the collator
   */
  public Collator getCollator() {
    return this.collator;
  }

  /**
   * @return the rewrite method
   */
  public MultiTermQuery.RewriteMethod getMultiTermRewriteMethod() {
    return multiTermRewriteMethod;
  }
}
