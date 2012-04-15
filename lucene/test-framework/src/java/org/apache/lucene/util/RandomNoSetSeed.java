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

import java.util.Random;

/**
 * A random with a delegate, preventing calls to {@link Random#setSeed(long)} and
 * permitting end-of-lifecycle markers. 
 */
@SuppressWarnings("serial")
final class RandomNoSetSeed extends Random {
  private final Random delegate;
  
  /** 
   * If <code>false</code>, the object is dead. Any calls to any method will result
   * in an exception. 
   */
  private volatile boolean alive = true;
  
  void setDead() {
    alive = false;
  }

  public RandomNoSetSeed(Random delegate) {
    super(0);
    this.delegate = delegate;
  }

  @Override
  protected int next(int bits) {
    throw new RuntimeException("Shouldn't be reachable.");
  }

  @Override
  public boolean nextBoolean() {
    checkAlive();
    return delegate.nextBoolean();
  }
  
  @Override
  public void nextBytes(byte[] bytes) {
    checkAlive();
    delegate.nextBytes(bytes);
  }
  
  @Override
  public double nextDouble() {
    checkAlive();
    return delegate.nextDouble();
  }
  
  @Override
  public float nextFloat() {
    checkAlive();
    return delegate.nextFloat();
  }
  
  @Override
  public double nextGaussian() {
    checkAlive();
    return delegate.nextGaussian();
  }
  
  @Override
  public int nextInt() {
    checkAlive();
    return delegate.nextInt();
  }
  
  @Override
  public int nextInt(int n) {
    checkAlive();
    return delegate.nextInt(n);
  }
  
  @Override
  public long nextLong() {
    checkAlive();
    return delegate.nextLong();
  }
  
  @Override
  public void setSeed(long seed) {
    // This is an interesting case of observing uninitialized object from an instance method
    // (this method is called from the superclass constructor). We allow it.
    if (seed == 0 && delegate == null) {
      return;
    }

    throw new RuntimeException(
        RandomNoSetSeed.class.getSimpleName() + 
        " prevents changing the seed of its random generators to assure repeatability" +
        " of tests. If you need a mutable instance of Random, create a new instance," +
        " preferably with the initial seed aquired from this Random instance."); 
  }

  @Override
  public String toString() {
    checkAlive();
    return delegate.toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    checkAlive();
    return delegate.equals(obj);
  }
  
  @Override
  public int hashCode() {
    checkAlive();
    return delegate.hashCode();
  }

  /**
   * Check the liveness status.
   */
  private void checkAlive() {
    if (!alive) {
      throw new RuntimeException("This Random is dead. Do not store references to " +
      		"Random instances, acquire an instance when you need one.");
    }
  }
}
