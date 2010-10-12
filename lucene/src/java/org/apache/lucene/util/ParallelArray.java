package org.apache.lucene.util;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @lucene.internal
 */
public abstract class ParallelArray<T extends ParallelArray<?>> {

    public final int size;
    protected final AtomicLong bytesUsed;

    protected ParallelArray(final int size, AtomicLong bytesUsed) {
      this.size = size;
      this.bytesUsed = bytesUsed;
      bytesUsed.addAndGet((size) * bytesPerEntry());

    }

    protected abstract int bytesPerEntry();
    
    public AtomicLong bytesUsed() {
      return bytesUsed;
    }
    
    public void deref() {
      bytesUsed.addAndGet((-size) * bytesPerEntry());
    }

    public abstract T newInstance(int size);
    
    public final T grow() {
      int newSize = ArrayUtil.oversize(size + 1, bytesPerEntry());
      T newArray = newInstance(newSize);
      copyTo(newArray, size);
      bytesUsed.addAndGet((newSize - size) * bytesPerEntry());
      return newArray;
    }

    protected abstract void copyTo(T toArray, int numToCopy);
}
