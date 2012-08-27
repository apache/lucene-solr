package org.apache.lucene.util.hash;
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
import java.util.Set;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NamedSPILoader;


/**
 * Base class for hashing functions that can be referred to by name.
 * Subclasses are expected to provide threadsafe implementations of the hash function
 * on the range of bytes referenced in the provided {@link BytesRef}
 * @lucene.experimental
 */
public abstract class HashFunction implements NamedSPILoader.NamedSPI {

  /**
   * Hashes the contents of the referenced bytes
   * @param bytes the data to be hashed
   * @return the hash of the bytes referenced by bytes.offset and length bytes.length
   */
  public abstract int hash(BytesRef bytes);
  
  private static final NamedSPILoader<HashFunction> loader =
    new NamedSPILoader<HashFunction>(HashFunction.class);

  private final String name;

  public HashFunction(String name) {
    NamedSPILoader.checkServiceName(name);
    this.name = name;
  }
  
  /** Returns this codec's name */
  @Override
  public final String getName() {
    return name;
  }
  
  /** looks up a hash function by name */
  public static HashFunction forName(String name) {
    return loader.lookup(name);
  }
  
  /** returns a list of all available hash function names */
  public static Set<String> availableHashFunctionNames() {
    return loader.availableServices();
  }
  
  /** 
   * Reloads the hash function list from the given {@link ClassLoader}.
   * Changes to the function list are visible after the method ends, all
   * iterators ({@link #availableHashFunctionNames()},...) stay consistent. 
   * 
   * <p><b>NOTE:</b> Only new functions are added, existing ones are
   * never removed or replaced.
   * 
   * <p><em>This method is expensive and should only be called for discovery
   * of new functions on the given classpath/classloader!</em>
   */
  public static void reloadHashFunctions(ClassLoader classloader) {
    loader.reload(classloader);
  }
  
  @Override
  public String toString() {
    return name;
  }  
}
