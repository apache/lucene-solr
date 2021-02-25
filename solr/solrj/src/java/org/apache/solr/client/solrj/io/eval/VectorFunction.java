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
package org.apache.solr.client.solrj.io.eval;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

@SuppressWarnings({"rawtypes"})
public class VectorFunction extends ArrayList {

  protected static final long serialVersionUID = 1L;

  private Object function;
  private Map context = new HashMap();

  @SuppressWarnings({"unchecked"})
  public VectorFunction(Object function, double[] results) {
    this.function = function;
    for(double d : results) {
      add(d);
    }
  }

  @SuppressWarnings({"unchecked"})
  public VectorFunction(Object function, List<Number> values) {
    this.function = function;
    addAll(values);
  }

  public Object getFunction() {
    return this.function;
  }

  @SuppressWarnings({"unchecked"})
  public void addToContext(Object key, Object value) {
    this.context.put(key, value);
  }

  public Object getFromContext(Object key) {
    return this.context.get(key);
  }

}
