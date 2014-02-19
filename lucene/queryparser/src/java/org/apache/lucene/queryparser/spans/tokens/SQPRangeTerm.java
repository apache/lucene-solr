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

public class SQPRangeTerm extends SQPTerminal {
  private final String start;
  private final String end;
  private final boolean startInclusive;
  private final boolean endInclusive;
  
  public SQPRangeTerm(String from, String to, boolean startInclusive, boolean endInclusive) {
    this.start = from;
    this.end = to;
    this.startInclusive = startInclusive;
    this.endInclusive = endInclusive;
  }
  
  public String getStart() {
    return start;
  }
  
  public String getEnd() {
    return end;
  }

  public boolean getStartInclusive() {
    return startInclusive;
  }
  
  public boolean getEndInclusive() {
    return endInclusive;
  }

  @Override
  public String getString() {
    StringBuilder sb = new StringBuilder();
    sb.append(start).append(" TO ").append(end);
    return sb.toString();
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((end == null) ? 0 : end.hashCode());
    result = prime * result + (endInclusive ? 1231 : 1237);
    result = prime * result + ((start == null) ? 0 : start.hashCode());
    result = prime * result + (startInclusive ? 1231 : 1237);
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
    if (!(obj instanceof SQPRangeTerm)) {
      return false;
    }
    SQPRangeTerm other = (SQPRangeTerm) obj;
    if (end == null) {
      if (other.end != null) {
        return false;
      }
    } else if (!end.equals(other.end)) {
      return false;
    }
    if (endInclusive != other.endInclusive) {
      return false;
    }
    if (start == null) {
      if (other.start != null) {
        return false;
      }
    } else if (!start.equals(other.start)) {
      return false;
    }
    if (startInclusive != other.startInclusive) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("SQPRangeTerm [start=");
    builder.append(start);
    builder.append(", end=");
    builder.append(end);
    builder.append(", startInclusive=");
    builder.append(startInclusive);
    builder.append(", endInclusive=");
    builder.append(endInclusive);
    builder.append("]");
    return builder.toString();
  }
}
