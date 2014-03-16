package org.apache.lucene.server.params;

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

import java.util.HashMap;
import java.util.Map;

/** Allows one parameter in a struct (usually named class)
 *  to define/refine the type of the struct.  For example,
 *  this is used for a generic "similarity": the "class"
 *  param will specify a concrete class (DefaultSim,
 *  BM25Sim, etc.), and switch the struct to other types
 *  (because each sim impl takes different params) based on
 *  that. */

@SuppressWarnings("rawtypes")
public class PolyType extends Type {

  /** Maps parameter name to the {@link PolyEntry}
   *  describing the sub type. */
  public final Map<String,PolyEntry> types = new HashMap<String,PolyEntry>();

  /** Not yet used but ... could be used in the future
   *  to allow custom (plugin) classes to be accepted. */
  public final Class baseClass;

  /** Default value, or null. */
  public final String defaultClassName;

  /** Describes one sub type. */
  public static class PolyEntry {

    /** Name of the sub-type, e.g. BM25Similarity. */
    public final String name;

    /** Defines the sub type. */
    public final StructType type;

    /** Description of this entry (English). */
    public final String desc;

    /** Creates this, folding the provided params into a
     *  new {@link StructType}. */
    public PolyEntry(String name, String desc, Param... params) {
      this(name, desc, new StructType(params));
    }

    /** Creates this. */
    public PolyEntry(String name, String desc, StructType type) {
      this.name = name;
      this.desc = desc;
      this.type = type;
    }
  }

  @Override
  public void validate(Object o) {
  }

  /** No default value. */
  public PolyType(Class baseClass, PolyEntry... entries) {
    this(baseClass, null, entries);
  }

  /** With default value. */
  public PolyType(Class baseClass, String defaultClassName, PolyEntry... entries) {
    this.baseClass = baseClass;
    this.defaultClassName = defaultClassName;
    for(PolyEntry e : entries) {
      if (types.containsKey(e.name)) {
        throw new IllegalArgumentException("name \"" + e.name + "\" appears more than once");
      }
      types.put(e.name, e);
    }
  }
}
