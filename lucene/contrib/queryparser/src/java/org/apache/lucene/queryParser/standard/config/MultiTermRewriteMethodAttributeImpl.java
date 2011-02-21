package org.apache.lucene.queryParser.standard.config;

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

import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.standard.processors.ParametricRangeQueryNodeProcessor;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.MultiTermQuery.RewriteMethod;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute is used by {@link ParametricRangeQueryNodeProcessor} processor
 * and should be defined in the {@link QueryConfigHandler} used by this
 * processor. It basically tells the processor which {@link RewriteMethod} to
 * use. <br/>
 * 
 * @see MultiTermRewriteMethodAttribute
 */
public class MultiTermRewriteMethodAttributeImpl extends AttributeImpl
    implements MultiTermRewriteMethodAttribute {
  
  private MultiTermQuery.RewriteMethod multiTermRewriteMethod = MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;

  public MultiTermRewriteMethodAttributeImpl() {
    // empty constructor
  }

  public void setMultiTermRewriteMethod(MultiTermQuery.RewriteMethod method) {
    multiTermRewriteMethod = method;
  }

  public MultiTermQuery.RewriteMethod getMultiTermRewriteMethod() {
    return multiTermRewriteMethod;
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copyTo(AttributeImpl target) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object other) {

    if (other instanceof MultiTermRewriteMethodAttributeImpl
        && ((MultiTermRewriteMethodAttributeImpl) other).multiTermRewriteMethod == this.multiTermRewriteMethod) {

      return true;

    }

    return false;

  }

  @Override
  public int hashCode() {
    return multiTermRewriteMethod.hashCode();
  }

  @Override
  public String toString() {
    return "<multiTermRewriteMethod multiTermRewriteMethod="
        + this.multiTermRewriteMethod + "/>";
  }

}
