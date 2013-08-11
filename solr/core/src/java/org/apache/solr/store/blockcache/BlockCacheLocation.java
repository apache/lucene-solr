package org.apache.solr.store.blockcache;

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

import java.util.concurrent.atomic.AtomicBoolean;

public class BlockCacheLocation {
  
  private int block;
  private int bankId;
  private long lastAccess = System.currentTimeMillis();
  private long accesses;
  private AtomicBoolean removed = new AtomicBoolean(false);
  
  public void setBlock(int block) {
    this.block = block;
  }
  
  public void setBankId(int bankId) {
    this.bankId = bankId;
  }
  
  public int getBlock() {
    return block;
  }
  
  public int getBankId() {
    return bankId;
  }
  
  public void touch() {
    lastAccess = System.currentTimeMillis();
    accesses++;
  }
  
  public long getLastAccess() {
    return lastAccess;
  }
  
  public long getNumberOfAccesses() {
    return accesses;
  }
  
  public boolean isRemoved() {
    return removed.get();
  }
  
  public void setRemoved(boolean removed) {
    this.removed.set(removed);
  }
  
}