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

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;

/**
 * Implements a combination of {@link java.util.WeakHashMap} and
 * {@link java.util.IdentityHashMap}.
 * Useful for caches that need to key off of a {@code ==} comparison
 * instead of a {@code .equals}.
 * 
 * <p>This class is not a general-purpose {@link java.util.Map}
 * implementation! It intentionally violates
 * Map's general contract, which mandates the use of the equals method
 * when comparing objects. This class is designed for use only in the
 * rare cases wherein reference-equality semantics are required.
 * 
 * <p><b>Note that this implementation is not synchronized.</b>
 *
 * <p>This implementation was forked from <a href="http://cxf.apache.org/">Apache CXF</a>
 * but modified to <b>not</b> implement the {@link java.util.Map} interface and
 * without any set/iterator views on it, as those are error-prone
 * and inefficient, if not implemented carefully. Lucene's implementation also
 * supports {@code null} keys, but those are never weak!
 *
 * @lucene.internal
 */
public class WeakIdentityHashMap<K,V> {
  final ReferenceQueue<Object> queue = new ReferenceQueue<Object>(); // pkg-private for inner class
  private final HashMap<IdentityWeakReference, V> backingStore;

  public WeakIdentityHashMap() {
    backingStore = new HashMap<IdentityWeakReference, V>();
  }

  public WeakIdentityHashMap(int initialCapacity) {
    backingStore = new HashMap<IdentityWeakReference,V>(initialCapacity);
  }

  public WeakIdentityHashMap(int initialCapacity, float loadFactor) {
    backingStore = new HashMap<IdentityWeakReference,V>(initialCapacity, loadFactor);
  }

  public void clear() {
    backingStore.clear();
    reap();
  }

  public boolean containsKey(Object key) {
    reap();
    return backingStore.containsKey(new IdentityWeakReference(key));
  }

  public boolean containsValue(Object value)  {
    reap();
    return backingStore.containsValue(value);
  }

  public V get(Object key) {
    reap();
    return backingStore.get(new IdentityWeakReference(key));
  }

  public V put(K key, V value) {
    reap();
    return backingStore.put(new IdentityWeakReference(key), value);
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public V remove(Object key) {
    try {
      reap();
      return backingStore.remove(new IdentityWeakReference(key));
    } finally {
      reap();
    }
  }

  public int size() {
    if (backingStore.isEmpty())
      return 0;
    reap();
    return backingStore.size();
  }

  private void reap() {
    Reference<?> zombie;
    while ((zombie = queue.poll()) != null) {
      backingStore.remove(zombie);
    }
  }

  final class IdentityWeakReference extends WeakReference<Object> {
    private final int hash;
    
    IdentityWeakReference(Object obj) {
      super(obj == null ? NULL : obj, queue);
      hash = System.identityHashCode(obj);
    }

    public int hashCode() {
      return hash;
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof WeakReference) {
        final WeakReference ref = (WeakReference)o;
        if (this.get() == ref.get()) {
          return true;
        }
      }
      return false;
    }
  }
  
  // we keep a hard reference to our NULL key, so this map supports null keys that never get GCed:
  static final Object NULL = new Object();
}

