package org.apache.lucene.facet.search;

import java.util.concurrent.ConcurrentLinkedQueue;

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

/**
 * An TemporaryObjectAllocator is an object which manages large, reusable,
 * temporary objects needed during multiple concurrent computations. The idea
 * is to remember some of the previously allocated temporary objects, and
 * reuse them if possible to avoid constant allocation and garbage-collection
 * of these objects. 
 * <P>
 * This technique is useful for temporary counter arrays in faceted search
 * (see {@link FacetsAccumulator}), which can be reused across searches instead
 * of being allocated afresh on every search.
 * <P>
 * A TemporaryObjectAllocator is thread-safe.
 * 
 * @lucene.experimental
 */
public abstract class TemporaryObjectAllocator<T> {

  // In the "pool" we hold up to "maxObjects" old objects, and if the pool
  // is not empty, we return one of its objects rather than allocating a new
  // one.
  ConcurrentLinkedQueue<T> pool = new ConcurrentLinkedQueue<T>();  
  int maxObjects;

  /**
   * Construct an allocator for objects of a certain type, keeping around a
   * pool of up to <CODE>maxObjects</CODE> old objects.
   * <P>
   * Note that the pool size only restricts the number of objects that hang
   * around when not needed, but <I>not</I> the maximum number of objects
   * that are allocated when actually is use: If a number of concurrent
   * threads ask for an allocation, all of them will get an object, even if 
   * their number is greater than maxObjects. If an application wants to
   * limit the number of concurrent threads making allocations, it needs to
   * do so on its own - for example by blocking new threads until the
   * existing ones have finished. If more than maxObjects are freed, only
   * maxObjects of them will be kept in the pool - the rest will not and
   * will eventually be garbage-collected by Java.
   * <P>
   * In particular, when maxObjects=0, this object behaves as a trivial
   * allocator, always allocating a new array and never reusing an old one. 
   */
  public TemporaryObjectAllocator(int maxObjects) {
    this.maxObjects = maxObjects;
  }

  /**
   * Subclasses must override this method to actually create a new object
   * of the desired type.
   * 
   */
  protected abstract T create();

  /**
   * Subclasses must override this method to clear an existing object of
   * the desired type, to prepare it for reuse. Note that objects will be
   * cleared just before reuse (on allocation), not when freed.
   */
  protected abstract void clear(T object);

  /**
   * Allocate a new object. If there's a previously allocated object in our
   * pool, we return it immediately. Otherwise, a new object is allocated.
   * <P>
   * Don't forget to call {@link #free(Object)} when you're done with the object,
   * to return it to the pool. If you don't, memory is <I>not</I> leaked,
   * but the pool will remain empty and a new object will be allocated each
   * time (just like the maxArrays=0 case). 
   */
  public final T allocate() {
    T object = pool.poll();
    if (object==null) {
      return create();
    }
    clear(object);
    return object;
  }

  /**
   * Return a no-longer-needed object back to the pool. If we already have
   * enough objects in the pool (maxObjects as specified in the constructor),
   * the array will not be saved, and Java will eventually garbage collect
   * it.
   * <P>
   * In particular, when maxArrays=0, the given array is never saved and
   * free does nothing.
   */
  public final void free(T object) {
    if (pool.size() < maxObjects && object != null) {
      pool.add(object);
    }
  }

}
