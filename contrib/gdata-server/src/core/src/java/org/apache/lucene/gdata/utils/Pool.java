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

/**
 * Basic interface to be implemented by ObjectPool implementations. Pools should
 * provide a constructor with a
 * {@link org.apache.lucene.gdata.utils.PoolObjectFactory} as a mandatory
 * parameter to create and destory the pooled objects.
 * 
 * @see org.apache.lucene.gdata.utils.PoolObjectFactory
 * 
 * @author Simon Willnauer
 * @param <Type> -
 *            the type of the pooled objects
 * 
 */
public interface Pool<Type> {
    /**
     * Return an object from the pool or create one if the pool is empty.
     * 
     * @return - a pooled object
     */
    public abstract Type aquire();

    /**
     * Adds a previously aquired object to the pool. If the pool has already
     * been closed or if the pool has already reached his size the released
     * object will be destroyed using
     * {@link PoolObjectFactory#destroyInstance(Object)} method.
     * 
     * @param type -
     *            the previously aquired object
     */
    public abstract void release(final Type type);

    /**
     * @return - the defined size of the pool
     */
    public abstract int getSize();

    /**
     * @return - the expire time of the objects in the pool if defined
     */
    public abstract long getExpireTime();

    /**
     * @return <code>true</code> if and only if the pool uses an expire
     *         mechanismn, otherwith <code>false</code>
     */
    public abstract boolean expires();

    /**
     * releases all pooled objects using
     * {@link PoolObjectFactory#destroyInstance(Object)} method. The pool can not
     * be reused after this method has been called
     */
    public abstract void destroy();

}
