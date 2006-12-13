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

package org.apache.lucene.gdata.server.registry.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple configuration class storing properties as key with defined property
 * values as values in a <code>Map<String,String></code>. As a map cannot
 * contain duplicate keys the first use of a key can not be replaced. If a key
 * is used twice a {@link java.lang.IllegalArgumentException} will be thrown.
 * @see Map
 * @author Simon Willnauer
 * 
 */
public class ComponentConfiguration {
    private final Map<String, String> configMap;

    /**
     * Creates a new ComponentConfiguration object and initializes the internal
     * map.
     */
    public ComponentConfiguration() {
        super();
        this.configMap = new HashMap<String, String>();
    }

    /**
     * Stores a key / value pair as a property. If a key is used twice the first
     * call will set the key / value pair. Any subsequent calls with a already
     * set key will throw a IllegalArgumentException.
     * 
     * @param key -
     *            the property as a key
     * @param value -
     *            the value for the key
     *@see Map#put(Object, Object)
     */
    public void set(final String key, final String value) {
        if (this.configMap.containsKey(key))
            throw new IllegalArgumentException("key has already been used");
        this.configMap.put(key, value);
    }

    /**
     * Returns the value of the key or <code>null</code> if the key is not set.
     * @param key - the key
     * @return - the value for the key or <code>null</code> if the key is not set.
     * @see Map#get(java.lang.Object) 
     */
    public String get(final String key) {
        return this.configMap.get(key);
    }

    /**
     * @param key - a string key
     * @return - <code>true</code> if the key is set, otherwise <code>false</code>
     * @see Map#containsKey(java.lang.Object)
     */
    public boolean contains(String key) {
        return this.configMap.containsKey(key);
    }

}
