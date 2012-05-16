package org.apache.lucene.index;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.index.FieldInfo.IndexOptions;

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
  private final Collection<FieldInfo> values; // for an unmodifiable iterator
  
  public ReadOnlyFieldInfos(FieldInfo[] infos) {
    boolean hasVectors = false;
    boolean hasProx = false;
    boolean hasFreq = false;
    boolean hasNorms = false;
    boolean hasDocValues = false;
    
    for (FieldInfo info : infos) {
      assert !byNumber.containsKey(info.number);
      byNumber.put(info.number, info);
      assert !byName.containsKey(info.name);
      byName.put(info.name, info);
      
      hasVectors |= info.hasVectors();
      hasProx |= info.isIndexed() && info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      hasFreq |= info.isIndexed() && info.getIndexOptions() != IndexOptions.DOCS_ONLY;
      hasNorms |= info.hasNorms();
      hasDocValues |= info.hasDocValues();
    }
    
    this.hasVectors = hasVectors;
    this.hasProx = hasProx;
    this.hasFreq = hasFreq;
    this.hasNorms = hasNorms;
    this.hasDocValues = hasDocValues;
    this.values = Collections.unmodifiableCollection(byNumber.values());
  }
  
  @Override
  public boolean hasFreq() {
    return hasFreq;
  }
  
  @Override
  public boolean hasProx() {
    return hasProx;
  }
  
  @Override
  public boolean hasVectors() {
    return hasVectors;
  }
  
  @Override
  public boolean hasNorms() {
    return hasNorms;
  }
  
  @Override
  public boolean hasDocValues() {
    return hasDocValues;
  }
  
  @Override
  public int size() {
    assert byNumber.size() == byName.size();
    return byNumber.size();
  }
  
  @Override
  public Iterator<FieldInfo> iterator() {
    return values.iterator();
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
    return new ReadOnlyFieldInfos(infos);
  }

}
