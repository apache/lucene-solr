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

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** Java's builtin ThreadLocal has a serious flaw:
 *  it can take an arbitrarily long amount of time to
 *  dereference the things you had stored in it, even once the
 *  ThreadLocal instance itself is no longer referenced.
 *  This is because there is single, master map stored for
 *  each thread, which all ThreadLocals share, and that
 *  master map only periodically purges "stale" entries.
 *
 *  While not technically a memory leak, because eventually
 *  the memory will be reclaimed, it can take a long time
 *  and you can easily hit OutOfMemoryError because from the
 *  GC's standpoint the stale entries are not reclaimable.
 * 
 *  This class works around that, by only enrolling
 *  WeakReference values into the ThreadLocal, and
 *  separately holding a hard reference to each stored
 *  value.  When you call {@link #close}, these hard
 *  references are cleared and then GC is freely able to
 *  reclaim space by objects stored in it.
 *
 *  We can not rely on {@link ThreadLocal#remove()} as it
 *  only removes the value for the caller thread, whereas
 *  {@link #close} takes care of all
 *  threads.  You should not call {@link #close} until all
 *  threads are done using the instance.
 *
 * @lucene.internal
 */

public class CloseableThreadLocal<T> implements Closeable {

  private ThreadLocal<WeakReference<T>> t = new ThreadLocal<WeakReference<T>>();

  private Map<Thread,T> hardRefs = new HashMap<Thread,T>();
  
  protected T initialValue() {
    return null;
  }
  
  public T get() {
    WeakReference<T> weakRef = t.get();
    if (weakRef == null) {
      T iv = initialValue();
      if (iv != null) {
        set(iv);
        return iv;
      } else
        return null;
    } else {
      return weakRef.get();
    }
  }

  public void set(T object) {

    t.set(new WeakReference<T>(object));

    synchronized(hardRefs) {
      hardRefs.put(Thread.currentThread(), object);

      // Purge dead threads
      for (Iterator<Thread> it = hardRefs.keySet().iterator(); it.hasNext();) {
        final Thread t = it.next();
        if (!t.isAlive())
          it.remove();
      }
    }
  }

  public void close() {
    // Clear the hard refs; then, the only remaining refs to
    // all values we were storing are weak (unless somewhere
    // else is still using them) and so GC may reclaim them:
    hardRefs = null;
    // Take care of the current thread right now; others will be
    // taken care of via the WeakReferences.
    if (t != null) {
      t.remove();
    }
    t = null;
  }
}
