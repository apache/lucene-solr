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
 * <p>
 * This interface enables {@link org.apache.lucene.gdata.utils.Pool} users to
 * build a custom creation and destroy mechanismn for pooled objects.
 * Implementations can use standart creation to prevent the pool from using
 * reflection to create objects of the specific type. This implementation
 * seperates the Pool implementation from the creation or the destruction of a
 * pooled type.
 * </p>
 * <p>
 * The destroy method can be used to close datasource connections or release
 * resources if the object will be removed from the pool
 * </p>
 * 
 * 
 * @see org.apache.lucene.gdata.utils.Pool
 * @author Simon Willnauer
 * @param <Type> -
 *            the type to be created
 * 
 */
public interface PoolObjectFactory<Type> {

    /**
     * @return an instance of the specified Type
     */
    public abstract Type getInstance();

    /**
     * destroys the given instance
     * @param type - the object to destroy / release all resources
     */
    public abstract void destroyInstance(Type type);

}
