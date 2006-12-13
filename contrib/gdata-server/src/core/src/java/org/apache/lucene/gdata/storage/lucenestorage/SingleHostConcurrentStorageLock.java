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

package org.apache.lucene.gdata.storage.lucenestorage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Simon Willnauer
 *
 */
public class SingleHostConcurrentStorageLock implements ConcurrentStorageLock {
    private volatile static ConcurrentStorageLock INSTANCE = null;
    private final Map<String,Thread> locks;
    private final Map<Thread,String> threads;
    private final ReentrantReadWriteLock synLock = new ReentrantReadWriteLock();
    private final Lock readLock = this.synLock.readLock();
    private final Lock writeLock = this.synLock.writeLock();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    /**
     * 
     */
    private SingleHostConcurrentStorageLock() {
        super();
        this.locks = new HashMap<String,Thread>(10);
        this.threads = new HashMap<Thread,String>(10);
    }
    protected static synchronized ConcurrentStorageLock getConcurrentStorageLock(){
        if(INSTANCE == null)
            INSTANCE = new SingleHostConcurrentStorageLock();
        return INSTANCE;
    }
    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.ConcurrentStorageLock#setLock(java.lang.String)
     */
    public boolean setLock(String key) {
       this.writeLock.lock();
       try{
           if(this.isClosed.get())
               throw new IllegalStateException("Lock has been closed");
           Thread t = Thread.currentThread();
           if(this.threads.containsKey(t))
               throw new ConcurrencyException("one thread must not obtain more than one lock -- single thread can not modify more than one resource");
           if(this.locks.containsKey(key)){
               return false;
           }
           this.locks.put(key, t);
           this.threads.put(t,key);
           return true;
           
       }finally{
           this.writeLock.unlock();
       }
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.ConcurrentStorageLock#releaseLock(java.lang.String)
     */
    public boolean releaseLock(String key) {
        this.writeLock.lock();
        try{
            if(this.isClosed.get())
                throw new IllegalStateException("Lock has been closed");
            Thread t = Thread.currentThread();
            if(!this.threads.containsKey(t))
                return false;
            
            if(!this.locks.containsKey(key))
                return false;
            if(t != this.locks.get(key))
                throw new ConcurrencyException("Illegal lock access -- current thread is not owner");
            this.locks.remove(key);
            this.threads.remove(t);
            return true;
            
        }finally{
            this.writeLock.unlock();
        }
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.ConcurrentStorageLock#releaseThreadLocks()
     */
    public boolean releaseThreadLocks() {
        this.writeLock.lock();
        try{
            if(this.isClosed.get())
                throw new IllegalStateException("Lock has been closed");
            Thread t = Thread.currentThread();
            if(!this.threads.containsKey(t))
                return false;
            String key = this.threads.get(t);
            this.threads.remove(t);
            if(!this.locks.containsKey(key))
                return false;
            this.locks.remove(key);
            return true;
            
        }finally{
            this.writeLock.unlock();
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.ConcurrentStorageLock#isKeyLocked(java.lang.String)
     */
    public boolean isKeyLocked(String key) {
        this.readLock.lock();
        try{
            if(this.isClosed.get())
                throw new IllegalStateException("Lock has been closed");
           return this.locks.containsKey(key);
        }finally{
            this.readLock.unlock();
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.ConcurrentStorageLock#close()
     */
    public void close() {
        this.writeLock.lock();
        try{
            if(this.isClosed.get())
                throw new IllegalStateException("Lock has been closed");
            this.isClosed.set(true);
            this.locks.clear();
            this.threads.clear();
            INSTANCE = new SingleHostConcurrentStorageLock();
        }finally{
            this.writeLock.unlock();
        }
    }
    
    
    protected void forceClear(){
        this.writeLock.lock();
        try{
            if(this.isClosed.get())
                throw new IllegalStateException("Lock has been closed");
            this.locks.clear();
            this.threads.clear();
            
        }finally{
            this.writeLock.unlock();
        }
    }
    static class ConcurrencyException extends RuntimeException{
      
        private static final long serialVersionUID = 6388236477729760962L;

        ConcurrencyException(String message){
            super(message);
        }
    }
}
