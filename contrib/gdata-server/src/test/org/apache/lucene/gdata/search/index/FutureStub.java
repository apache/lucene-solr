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

package org.apache.lucene.gdata.search.index;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Simon Willnauer
 *
 */
public class FutureStub<T> implements Future<T>{
    T object;
    CountDownLatch latch;
    boolean wait;
    
    /**
     * 
     */
    public FutureStub(T returnObject, CountDownLatch latch) {
        this(returnObject,latch,false);
    }
    /**
     * 
     */
    public FutureStub(T returnObject, CountDownLatch latch, boolean wait) {
        super();
        this.object = returnObject;
        this.latch =latch;
        this.wait =wait;
    }
    public FutureStub(T returnObject) {
        this(returnObject,null);
    }

    /**
     * @see java.util.concurrent.Future#cancel(boolean)
     */
    public boolean cancel(boolean arg0) {

        return false;
    }

    /**
     * @see java.util.concurrent.Future#isCancelled()
     */
    public boolean isCancelled() {

        return false;
    }

    /**
     * @see java.util.concurrent.Future#isDone()
     */
    public boolean isDone() {

        return true;
    }

    /**
     * @see java.util.concurrent.Future#get()
     */
    public T get() throws InterruptedException, ExecutionException {
        doLatch(); 
        return this.object;
    }

    /**
     * @see java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)
     */
    public T get(long arg0, TimeUnit arg1) throws InterruptedException,
            ExecutionException, TimeoutException {
       doLatch();
        return this.object;
    }
    
    private void doLatch() throws InterruptedException{
        if(latch != null){
            if(!wait)
            this.latch.countDown();
            else
                this.latch.await(5000,TimeUnit.MILLISECONDS);
        }
    }

}
