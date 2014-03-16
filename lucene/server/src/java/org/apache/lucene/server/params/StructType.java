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

import net.minidev.json.JSONObject;

/** Type for a structure containing named typed parameters. */
public class StructType extends Type {

  /** Parameters contained in this struct. */
  public final Map<String,Param> params = new HashMap<String,Param>();

  /** Sole constructor. */
  public StructType(Param... params) {
    boolean sawPoly = false;
    for(Param p : params) {
      if (this.params.containsKey(p.name)) {
        throw new IllegalArgumentException("param name \"" + p.name + "\" appears more than once");
      }
      if (p.type instanceof PolyType) {
        if (sawPoly) {
          throw new IllegalArgumentException("only one PolyType per struct");
        }
        sawPoly = true;
      }
      this.params.put(p.name, p);
    }
  }

  /** Add another parameter. */
  public void addParam(Param param) {
    if (params.containsKey(param.name)) {
      throw new IllegalArgumentException("param name \"" + param.name + "\" already exists");
    }
    params.put(param.name, param);
  }

  @Override
  public void validate(Object _o) {
    if (!(_o instanceof JSONObject)) {
      throw new IllegalArgumentException("expected struct but got " + _o.getClass());
    }
  }
}
