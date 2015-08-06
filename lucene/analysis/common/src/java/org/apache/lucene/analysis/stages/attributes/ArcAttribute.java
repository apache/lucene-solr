package org.apache.lucene.analysis.stages.attributes;

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

import org.apache.lucene.util.Attribute;

public class ArcAttribute implements Attribute, Cloneable {
  private int from;
  private int to;
  
  public ArcAttribute() {
  }

  public int from() {
    return from;
  }

  public int to() {
    return to;
  }

  public void set(int from, int to) {
    this.from = from;
    this.to = to;
  }

  public void clear() {
    from = 0;
    to = 0;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof ArcAttribute) {
      ArcAttribute o = (ArcAttribute) other;
      return o.from == from && o.to == to;
    }
    
    return false;
  }

  @Override
  public int hashCode() {
    int code = from;
    code = code * 31 + to;
    return code;
  } 
  
  public void copyTo(Attribute target) {
    ArcAttribute t = (ArcAttribute) target;
    t.set(from, to);
  }  
}
