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

public class SQPTerm extends SQPTerminal {
  private final String string;
  private boolean isQuoted = false;

  public SQPTerm(String string, boolean isQuoted) {
    this.string = string;
    this.isQuoted = isQuoted;
  }
  
  public String getString() {
    return string;
  }

  public void setIsQuoted(boolean b) {
    isQuoted = b;
  }
  
  public boolean isQuoted() {
    return isQuoted;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (isQuoted ? 1231 : 1237);
    result = prime * result + ((string == null) ? 0 : string.hashCode());
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
    if (!(obj instanceof SQPTerm)) {
      return false;
    }
    SQPTerm other = (SQPTerm) obj;
    if (isQuoted != other.isQuoted) {
      return false;
    }
    if (string == null) {
      if (other.string != null) {
        return false;
      }
    } else if (!string.equals(other.string)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("SQPTerm [string=");
    builder.append(string);
    builder.append(", isQuoted=");
    builder.append(isQuoted);
    builder.append("]");
    return builder.toString();
  }
}
