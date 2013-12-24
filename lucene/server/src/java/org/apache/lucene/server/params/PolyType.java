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
 *  to define/refine the type of the struct (like
 *  polymorphism). For example, this is used for a generic
 *  "similarity": the "class" param will specify a concrete
 *  class (DefaultSim, BM25Sim, etc.), and switch the struct
 *  to other types (because each sim impl takes different
 *  params) based on that.  */

@SuppressWarnings("rawtypes")
public class PolyType extends Type {

  public final Map<String,PolyEntry> types = new HashMap<String,PolyEntry>();

  // NOTE: not yet used but ... could be used in the future
  // to allow custom (plugin) classes to be accepted
  public final Class baseClass;

  public static class PolyEntry {
    public final String name;
    public final StructType type;
    public final String desc;

    public PolyEntry(String name, String desc, Param... params) {
      this.name = name;
      this.desc = desc;
      this.type = new StructType(params);
    }

    public PolyEntry(String name, String desc, StructType type) {
      this.name = name;
      this.desc = desc;
      this.type = type;
    }
  }

  @Override
  public void validate(Object o) {
  }

  public PolyType(Class baseClass, PolyEntry... entries) {
    this.baseClass = baseClass;
    for(PolyEntry e : entries) {
      if (types.containsKey(e.name)) {
        throw new IllegalArgumentException("name \"" + e.name + "\" appears more than once");
      }
      types.put(e.name, e);
    }
  }
}
