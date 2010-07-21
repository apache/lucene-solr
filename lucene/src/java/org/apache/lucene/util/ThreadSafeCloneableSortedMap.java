package org.apache.lucene.util;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadSafeCloneableSortedMap<K, V> implements SortedMap<K, V>, Cloneable {

  private volatile SortedMap<K, V> copy;
  private Lock cloneLock = new ReentrantLock();
  private final SortedMap<K, V> delegate;

  private ThreadSafeCloneableSortedMap(SortedMap<K, V> delegate) {this.delegate = delegate;}

  public static <K, V> ThreadSafeCloneableSortedMap<K, V> getThreadSafeSortedMap(
      SortedMap<K, V> delegate) {
    return new ThreadSafeCloneableSortedMap<K, V>(delegate);
  }

  public SortedMap<K, V> getReadCopy() {
    SortedMap<K, V> m = copy;
    if (m != null) {
      return m;
    }

    // we have to clone
    cloneLock.lock();
    try {
      // check again - maybe a different thread was faster
      m = copy;
      if (m != null) {
        return m;
      }

      // still no copy there - create one now
      SortedMap<K, V> clone = clone(delegate);
      copy = clone;
      return clone;
    } finally {
      cloneLock.unlock();
    }

  }

  protected SortedMap<K, V> clone(SortedMap<K, V> map) {
    if (map instanceof TreeMap<?, ?>) {
      return (TreeMap<K,V>) ((TreeMap<?,?>) map).clone();
    }
    
    throw new IllegalArgumentException(map.getClass() + " not supported. Overwrite clone(SortedMap<K, V> map) in a custom subclass to support this map.");
  }
  
  private abstract static class Task<T> {
    abstract T run();
  }

  private final <T> T withLock(Task<T> task) {
    copy = null;
    cloneLock.lock();
    try {
      return task.run();
    } finally {
      cloneLock.unlock();
    }
  }

  @Override public Comparator<? super K> comparator() {
    return delegate.comparator();
  }

  @Override public SortedMap<K, V> subMap(K fromKey, K toKey) {
    return delegate.subMap(fromKey, toKey);
  }

  @Override public SortedMap<K, V> headMap(K toKey) {
    return delegate.headMap(toKey);
  }

  @Override public SortedMap<K, V> tailMap(K fromKey) {
    return delegate.tailMap(fromKey);
  }

  @Override public K firstKey() {
    return delegate.firstKey();
  }

  @Override public K lastKey() {
    return delegate.lastKey();
  }

  @Override public int size() {
    return delegate.size();
  }

  @Override public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override public boolean containsKey(Object key) {
    return delegate.containsKey(key);
  }

  @Override public boolean containsValue(Object value) {
    return delegate.containsValue(value);
  }

  @Override public V get(Object key) {
    return delegate.get(key);
  }

  @Override public V put(final K key, final V value) {
    return withLock(new Task<V>() {
      @Override V run() {return delegate.put(key, value);}
    });
  }

  @Override public V remove(final Object key) {
    return withLock(new Task<V>() {
      @Override V run() {return delegate.remove(key);}
    });
  }

  @Override public void putAll(final Map<? extends K, ? extends V> m) {
    withLock(new Task<V>() {
      @Override V run() {
        delegate.putAll(m);
        return null;
      }
    });
  }

  @Override public void clear() {
    withLock(new Task<V>() {
      @Override V run() {
        delegate.clear();
        return null;
      }
    });
  }

  //
  // nocommit : don't use these methods to modify the map.
  // TODO implement Set and Collection that acquire lock for modifications
  //
  @Override public Set<K> keySet() {
    return delegate.keySet();
  }

  @Override public Collection<V> values() {
    return delegate.values();
  }

  @Override public Set<Entry<K, V>> entrySet() {
    return delegate.entrySet();
  }
}
