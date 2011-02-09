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
import org.apache.lucene.queryParser.standard.processors.AllowLeadingWildcardProcessor;
import org.apache.lucene.util.AttributeImpl;

/**
 * This attribute is used by {@link AllowLeadingWildcardProcessor} processor and
 * must be defined in the {@link QueryConfigHandler}. It basically tells the
 * processor if it should allow leading wildcard. <br/>
 * 
 * @see org.apache.lucene.queryParser.standard.config.AllowLeadingWildcardAttribute
 */
public class AllowLeadingWildcardAttributeImpl extends AttributeImpl 
				implements AllowLeadingWildcardAttribute {

  private boolean allowLeadingWildcard = false;  // default in 2.9

  public void setAllowLeadingWildcard(boolean allowLeadingWildcard) {
    this.allowLeadingWildcard = allowLeadingWildcard;
  }

  public boolean isAllowLeadingWildcard() {
    return this.allowLeadingWildcard;
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

    if (other instanceof AllowLeadingWildcardAttributeImpl
        && ((AllowLeadingWildcardAttributeImpl) other).allowLeadingWildcard == this.allowLeadingWildcard) {

      return true;

    }

    return false;

  }

  @Override
  public int hashCode() {
    return this.allowLeadingWildcard ? -1 : Integer.MAX_VALUE;
  }

  @Override
  public String toString() {
    return "<allowLeadingWildcard allowLeadingWildcard="
        + this.allowLeadingWildcard + "/>";
  }

}
