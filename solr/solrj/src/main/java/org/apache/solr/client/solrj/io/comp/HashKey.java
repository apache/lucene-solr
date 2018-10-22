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
package org.apache.solr.client.solrj.io.comp;
import java.io.Serializable;

public class HashKey implements Serializable {

  private static final long serialVersionUID = 1;

  private Object[] parts;


  public HashKey(String value) {
    parts = value.split("::");
  }

  public HashKey(Object[] parts) {
    this.parts = parts;
  }

  public Object[] getParts() {
    return parts;
  }

  public int hashCode() {
    int h = 0;
    for(Object o : parts) {
      h+=o.hashCode();
    }

    return h;
  }

  public boolean equals(Object o) {
    HashKey h = (HashKey)o;
    for(int i=0; i<parts.length; i++) {
      if(!parts[i].equals(h.parts[i])) {
        return false;
      }
    }

    return true;
  }

  public String toString() {
    StringBuilder buf = new StringBuilder();
    for(int i=0; i<parts.length; i++) {
      if(i > 0) {
        buf.append("::");
      }
      buf.append(parts[i].toString());
    }

    return buf.toString();
  }
}