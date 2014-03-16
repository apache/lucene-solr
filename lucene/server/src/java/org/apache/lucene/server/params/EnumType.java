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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** Type for an enumeration. */
public class EnumType extends Type {

  /** Maps label to description. */
  final Map<String,String> values;

  /** Sole constructor, values is alternating label1,
   *  desc1, label2, desc2, ... */
  public EnumType(String ... values) {
    if ((values.length & 1) != 0) {
      throw new IllegalArgumentException("input must be value/desc pairs");
    }
    this.values = new HashMap<String,String>();
    for(int i=0;i<values.length;i+=2) {
      this.values.put(values[i], values[i+1]);
    }
  }

  @Override
  public void validate(Object o) {
    if (!(o instanceof String)) {
      throw new IllegalArgumentException("expected String but got " + o.getClass());
    }
    if (values.containsKey(o) == false) {
      String[] keys = new ArrayList<String>(values.keySet()).toArray(new String[values.size()]);
      throw new IllegalArgumentException("expected one of " + Arrays.toString(keys) + " but got \"" + o + "\"");
    }
  }
}
