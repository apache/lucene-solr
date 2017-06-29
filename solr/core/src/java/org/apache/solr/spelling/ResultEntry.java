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
package org.apache.solr.spelling;

public class ResultEntry {
  public Token token;
  public String suggestion;
  public int freq;
  ResultEntry(Token t, String s, int f) {
    token = t;
    suggestion = s;
    freq = f;    
  } 
  @Override
  public int hashCode() {  
    final int prime = 31;
    int result = 1;
    result = prime * result + freq;
    result = prime * result
        + ((suggestion == null) ? 0 : suggestion.hashCode());
    result = prime * result + ((token == null) ? 0 : token.hashCode());
    return result;
  }
  @Override
  public boolean equals(Object obj) {    
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ResultEntry other = (ResultEntry) obj;
    if (freq != other.freq) return false;
    if (suggestion == null) {
      if (other.suggestion != null) return false;
    } else if (!suggestion.equals(other.suggestion)) return false;
    if (token == null) {
      if (other.token != null) return false;
    } else if (!token.equals(other.token)) return false;
    return true;
  }
  
}
