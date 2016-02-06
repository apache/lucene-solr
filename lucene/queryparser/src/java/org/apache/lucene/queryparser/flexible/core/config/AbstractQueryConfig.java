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
package org.apache.lucene.queryparser.flexible.core.config;

import java.util.HashMap;

/**
 * <p>
 * This class is the base of {@link QueryConfigHandler} and {@link FieldConfig}.
 * It has operations to set, unset and get configuration values.
 * </p>
 * <p>
 * Each configuration is is a key-&gt;value pair. The key should be an unique
 * {@link ConfigurationKey} instance and it also holds the value's type.
 * </p>
 * 
 * @see ConfigurationKey
 */
public abstract class AbstractQueryConfig {
  
  final private HashMap<ConfigurationKey<?>, Object> configMap = new HashMap<>();
  
  AbstractQueryConfig() {
    // although this class is public, it can only be constructed from package
  }
  
  /**
   * Returns the value held by the given key.
   * 
   * @param <T> the value's type
   * 
   * @param key the key, cannot be <code>null</code>
   * 
   * @return the value held by the given key
   */
  @SuppressWarnings("unchecked")
  public <T> T get(ConfigurationKey<T> key) {
    
    if (key == null) {
      throw new IllegalArgumentException("key cannot be null!");
    }
    
    return (T) this.configMap.get(key);
    
  }

  /**
   * Returns true if there is a value set with the given key, otherwise false.
   * 
   * @param <T> the value's type
   * @param key the key, cannot be <code>null</code>
   * @return true if there is a value set with the given key, otherwise false
   */
  public <T> boolean has(ConfigurationKey<T> key) {
    
    if (key == null) {
      throw new IllegalArgumentException("key cannot be null!");
    }
    
    return this.configMap.containsKey(key);
    
  }
  
  /**
   * Sets a key and its value.
   * 
   * @param <T> the value's type
   * @param key the key, cannot be <code>null</code>
   * @param value value to set
   */
  public <T> void set(ConfigurationKey<T> key, T value) {
    
    if (key == null) {
      throw new IllegalArgumentException("key cannot be null!");
    }
    
    if (value == null) {
      unset(key);
      
    } else {
      this.configMap.put(key, value);
    }
    
  }

  /**
   * Unsets the given key and its value.
   * 
   * @param <T> the value's type
   * @param key the key
   * @return true if the key and value was set and removed, otherwise false
   */
  public <T> boolean unset(ConfigurationKey<T> key) {
    
    if (key == null) {
      throw new IllegalArgumentException("key cannot be null!");
    }
    
    return this.configMap.remove(key) != null;
    
  }

}
