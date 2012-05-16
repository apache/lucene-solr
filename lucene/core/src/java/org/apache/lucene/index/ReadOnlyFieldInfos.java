package org.apache.lucene.index;

import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
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

// nocommit: temporary
public final class ReadOnlyFieldInfos extends FieldInfos {
  // nocommit
  private final boolean hasFreq;
  private final boolean hasProx;
  private final boolean hasVectors;
  private final boolean hasNorms;
  private final boolean hasDocValues;
  
  private final SortedMap<Integer,FieldInfo> byNumber = new TreeMap<Integer,FieldInfo>();
  private final HashMap<String,FieldInfo> byName = new HashMap<String,FieldInfo>();
  
  public ReadOnlyFieldInfos(FieldInfo[] infos, boolean hasFreq, boolean hasProx, boolean hasVectors) {
    this.hasFreq = hasFreq;
    this.hasProx = hasProx;
    this.hasVectors = hasVectors;
    
    boolean hasNorms = false;
    boolean hasDocValues = false;
    
    for (FieldInfo info : infos) {
      assert !byNumber.containsKey(info.number);
      byNumber.put(info.number, info);
      assert !byName.containsKey(info.name);
      byName.put(info.name, info);
      
      hasNorms |= info.hasNorms();
      hasDocValues |= info.hasDocValues();
    }
    
    this.hasNorms = hasNorms;
    this.hasDocValues = hasDocValues;
  }
  
  public boolean hasFreq() {
    return hasFreq;
  }
  
  public boolean hasProx() {
    return hasProx;
  }
  
  public boolean hasVectors() {
    return hasVectors;
  }
  
  public boolean hasNorms() {
    return hasNorms;
  }
  
  public boolean hasDocValues() {
    return hasDocValues;
  }
  
  public int size() {
    assert byNumber.size() == byName.size();
    return byNumber.size();
  }
  
  public Iterator<FieldInfo> iterator() {
    return byNumber.values().iterator();
  }

  @Override
  public FieldInfo fieldInfo(String fieldName) {
    return byName.get(fieldName);
  }

  @Override
  public FieldInfo fieldInfo(int fieldNumber) {
    return (fieldNumber >= 0) ? byNumber.get(fieldNumber) : null;
  }

  // nocommit: probably unnecessary
  @Override
  public ReadOnlyFieldInfos clone() {
    FieldInfo infos[] = new FieldInfo[size()];
    int upto = 0;
    for (FieldInfo info : this) {
      infos[upto++] = info.clone();
    }
    return new ReadOnlyFieldInfos(infos, hasFreq, hasProx, hasVectors);
  }

}
