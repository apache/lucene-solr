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

public class SQPBooleanOpToken implements SQPToken{

  private final int type;
  
  public SQPBooleanOpToken(int type) {
    this.type = type;
  }
  
  public int getType() {
    return type;
  }

  public boolean isConj() {
    if (type == SpanQueryParserBase.CONJ_AND ||
        type == SpanQueryParserBase.CONJ_OR) {
      return true;
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + type;
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
    if (!(obj instanceof SQPBooleanOpToken)) {
      return false;
    }
    SQPBooleanOpToken other = (SQPBooleanOpToken) obj;
    if (type != other.type) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("SQPBooleanOpToken [type=");
    builder.append(type);
    builder.append("]");
    return builder.toString();
  }

  public static boolean isMod(int i) {
    if (i == SpanQueryParserBase.CONJ_AND ||
        i == SpanQueryParserBase.CONJ_OR) {
      return false;
    }
    return true;
  }
}
