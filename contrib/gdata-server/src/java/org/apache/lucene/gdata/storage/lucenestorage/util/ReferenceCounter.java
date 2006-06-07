/** 
 * Copyright 2004 The Apache Software Foundation 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 */ 
 
package org.apache.lucene.gdata.storage.lucenestorage.util; 
 
import java.util.concurrent.atomic.AtomicInteger; 
 
/** 
 * A reference counting utility. This is use to keep track of released objects 
 * of <code>Type</code>. 
 *  
 * @author Simon Willnauer 
 * @param <Type> - 
 *            the type of the object 
 *  
 */ 
public abstract class ReferenceCounter<Type> { 
    protected final Type resource; 
 
    private AtomicInteger refcounter = new AtomicInteger(); 
 
    /** 
     * @param resource - 
     *            the resouce to track 
     *  
     */ 
    public ReferenceCounter(Type resource) { 
        this.resource = resource; 
    } 
 
    /** 
     *  
     * Decrements the reference. If no references remain the 
     * {@link ReferenceCounter#close()} method will be inoked; 
     */ 
    public final void decrementRef() { 
        if (this.refcounter.decrementAndGet() == 0) 
            close(); 
    } 
 
    /** 
     * A custom implementation. Performs an action if no reference remaining 
     *  
     */ 
    protected abstract void close(); 
 
    /** 
     * Increments the reference 
     *  
     * @return the refernece object 
     */ 
    public final ReferenceCounter<Type> increamentReference() { 
        this.refcounter.incrementAndGet(); 
        return this; 
    } 
 
    /** 
     * @return - the resource to keep track of 
     */ 
    public final Type get() { 
        return this.resource; 
    } 
 
} 
