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

class SQPNotNearClause extends SQPClause {
  
  public static final int NOT_DEFAULT = 0;

  private final TYPE type;
  
  private final int notPre;
  private final int notPost;
  
  public SQPNotNearClause(int tokenStartOffset, int tokenEndOffset, TYPE type, 
      int notPre, int notPost) {
    super(tokenStartOffset, tokenEndOffset);
    this.type = type;
    this.notPre = notPre;
    this.notPost = notPost;
  }

  public TYPE getType() {
    return type;
  }

  public int getNotPre() {
    return notPre;
  }

  public int getNotPost() {
    return notPost;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + notPost;
    result = prime * result + notPre;
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (!(obj instanceof SQPNotNearClause)) {
      return false;
    }
    SQPNotNearClause other = (SQPNotNearClause) obj;
    if (notPost != other.notPost) {
      return false;
    }
    if (notPre != other.notPre) {
      return false;
    }
    if (type != other.type) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("SQPNotNearClause [type=");
    builder.append(type);
    builder.append(", notPre=");
    builder.append(notPre);
    builder.append(", notPost=");
    builder.append(notPost);
    builder.append("]");
    builder.append( getTokenOffsetStart() + ": " + getTokenOffsetEnd());
    return builder.toString();
  }
}
