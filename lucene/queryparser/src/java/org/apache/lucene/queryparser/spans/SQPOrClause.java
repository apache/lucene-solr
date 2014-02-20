package org.apache.lucene.queryparser.spans;

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

class SQPOrClause extends SQPClause {

  public static final int DEFAULT_MINIMUM_NUMBER_SHOULD_MATCH = 1;

  private int minimumNumberShouldMatch = DEFAULT_MINIMUM_NUMBER_SHOULD_MATCH;
  
  public SQPOrClause(int tokenOffsetStart, int tokenOffsetEnd) {
    super(tokenOffsetStart, tokenOffsetEnd);
  }
  
  public int getMinimumNumberShouldMatch() {
    return minimumNumberShouldMatch;
  }
  
  public void setMinimumNumberShouldMatch(int n) {
    minimumNumberShouldMatch = n;
  }
  
  public TYPE getType() {
    return TYPE.PAREN;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + minimumNumberShouldMatch;
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
    if (!(obj instanceof SQPOrClause)) {
      return false;
    }
    SQPOrClause other = (SQPOrClause) obj;
    if (minimumNumberShouldMatch != other.minimumNumberShouldMatch) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("SQPOrClause [minimumNumberShouldMatch=");
    builder.append(minimumNumberShouldMatch);
    builder.append("]");
    return builder.toString();
  }
}
