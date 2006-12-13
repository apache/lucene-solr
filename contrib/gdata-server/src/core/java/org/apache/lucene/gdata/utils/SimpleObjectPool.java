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

package org.apache.lucene.gdata.utils;

import java.util.Stack;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A Simple implementation of the {@link org.apache.lucene.gdata.utils.Pool}
 * interface using a {@link java.util.Stack} as a buffer for the pooled objects.
 * This implementation does not provide any timeout mechanismn. Objects will
 * stay inside the pool until the pool is destroyed.
 * <p>
 * If any object will be released e.g. handover to the pool and the pool has
 * already enought objects in the pool the released object will be destroyed. If
 * the pool is empty a new Object will be created.
 * </p>
 * <p>
 * This implementation does not track any references to the objects aquired by
 * any other resource. The objects must be destroyed manually if not released to
 * the pool after aquired.
 * </p>
 * 
 * @author Simon Willnauer
 * @param <Type>
 * 
 */
public class SimpleObjectPool<Type> implements Pool<Type> {
    private static final Log LOG = LogFactory.getLog(SimpleObjectPool.class);
    private volatile boolean isDestroyed = false;

    private final PoolObjectFactory<Type> factory;

    static final int DEFAULTSIZE = 5;
    static final int MINIMALSIZE = 1;

    private final int size;

    private final Stack<Type> pool;

    private final ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();

    private final Lock readLock = this.masterLock.readLock();

    private final Lock writeLock = this.masterLock.writeLock();

    /**
     * Constructs a new {@link SimpleObjectPool} and sets the ObjectFactory and the pool size 
     * @param size - the maximum size of the pool
     * @param factory - factory to create and destroy pooled objects
     * 
     */
    public SimpleObjectPool(int size, PoolObjectFactory<Type> factory) {
        if (factory == null)
            throw new IllegalArgumentException("Factory must not be null");
        this.factory = factory;
        this.size = size < MINIMALSIZE ? MINIMALSIZE : size;
        this.pool = new Stack<Type>();
        for (int i = 0; i < this.size; i++) {
            this.pool.push(this.factory.getInstance());
        }
    }
    /**
     * @param factory
     */
    public SimpleObjectPool(PoolObjectFactory<Type> factory) {
        this(DEFAULTSIZE,factory);
        
    }

    /**
     * @see org.apache.lucene.gdata.utils.Pool#aquire()
     */
    public Type aquire() {
        // fail if writelock is aquired
        if (this.readLock.tryLock()) {
            try {
                if (this.isDestroyed)
                    throw new IllegalStateException(
                            "The pool has already been closed");
                if (this.pool.isEmpty())
                    return this.factory.getInstance();
                return this.pool.pop();
            } finally {
                this.readLock.unlock();
            }
        }
        throw new IllegalStateException("The pool has already been closed");
    }

    /**
     *
     * @param type - generic type
     * @see org.apache.lucene.gdata.utils.Pool#release(Object)
     */
    public void release(Type type) {
        // fail if writelock is aquired
        if (this.readLock.tryLock()) {
            try {
                if (this.pool.size() < this.size && !this.isDestroyed)
                    this.pool.push(type);
                else
                    this.factory.destroyInstance(type);
            } finally {
                this.readLock.unlock();
            }
            return;
        }
        // enable object need to be destoryed
        this.factory.destroyInstance(type);
    }

    /**
     * @see org.apache.lucene.gdata.utils.Pool#getSize()
     */
    public int getSize() {

        return this.size;
    }

    /**
     * @see org.apache.lucene.gdata.utils.Pool#getExpireTime()
     */
    public long getExpireTime() {

        return 0;
    }

    /**
     * @see org.apache.lucene.gdata.utils.Pool#expires()
     */
    public boolean expires() {

        return false;
    }

    /**
     * @see org.apache.lucene.gdata.utils.Pool#destroy()
     */
    public void destroy() {
        this.writeLock.lock();
        try {
            if (this.isDestroyed)
                return;
            this.isDestroyed = true;
            LOG.info("Destroy all elements in the pool -- poolsize: "+this.pool.size());
            for (Type type : this.pool) {
                this.factory.destroyInstance(type);
            }
            this.pool.clear();
        } finally {
            this.writeLock.unlock();
        }

    }

}
