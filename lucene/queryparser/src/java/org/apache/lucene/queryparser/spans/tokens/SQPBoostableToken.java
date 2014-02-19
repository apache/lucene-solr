package org.apache.lucene.queryparser.spans.tokens;

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

import org.apache.lucene.queryparser.spans.SpanQueryParserBase;

public class SQPBoostableToken implements SQPToken {
  private float boost = SpanQueryParserBase.UNSPECIFIED_BOOST;

  public void setBoost(float boost) {
    this.boost = boost;
  }
  
  public float getBoost() {
    return boost;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Float.floatToIntBits(boost);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof SQPBoostableToken)) {
      return false;
    }
    SQPBoostableToken other = (SQPBoostableToken) obj;
    if (Float.floatToIntBits(boost) != Float.floatToIntBits(other.boost)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("SQPBoostableToken [boost=");
    builder.append(boost);
    builder.append("]");
    return builder.toString();
  }
}
