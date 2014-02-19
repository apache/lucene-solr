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

public abstract class SQPClause extends SQPBoostableToken {
  
  public static enum TYPE { PAREN, BRACKET, QUOTE, CURLY};
  private final int tokenOffsetStart;
  private int tokenOffsetEnd;

  public SQPClause(int tokenOffsetStart) {
    this.tokenOffsetStart = tokenOffsetStart;
  }
  
  public SQPClause(int tokenOffsetStart, int tokenOffsetEnd) {
    this(tokenOffsetStart);
    this.tokenOffsetEnd = tokenOffsetEnd;
  }
  
  public int getTokenOffsetStart() {
    return tokenOffsetStart;
  }
  
  public int getTokenOffsetEnd() {
    return tokenOffsetEnd;
  }
  
  public void setTokenOffsetEnd(int tokenOffsetEnd) {
    this.tokenOffsetEnd = tokenOffsetEnd;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + tokenOffsetStart;
    result = prime * result + tokenOffsetEnd;
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
    if (!(obj instanceof SQPClause)) {
      return false;
    }
    SQPClause other = (SQPClause) obj;
    if (tokenOffsetStart != other.tokenOffsetStart) {
      return false;
    }
    if (tokenOffsetEnd != other.tokenOffsetEnd) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("SQPClauseBase [charOffsetStart=");
    builder.append(tokenOffsetStart);
    builder.append(", tokenOffsetEnd=");
    builder.append(tokenOffsetEnd);
    builder.append("]");
    return builder.toString();
  }
}
