package org.apache.lucene.index;

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

public abstract class NumericDocValues {
  public abstract long get(int docID);
  
  public abstract long minValue();
  public abstract long maxValue();
  public abstract int size();
  
  public static final class EMPTY extends NumericDocValues {
    private final int size;
    
    public EMPTY(int size) {
      this.size = size;
    }

    @Override
    public long get(int docID) {
      return 0;
    }

    @Override
    public long minValue() {
      return 0;
    }

    @Override
    public long maxValue() {
      return 0;
    }

    @Override
    public int size() {
      return size;
    }
  };
}
