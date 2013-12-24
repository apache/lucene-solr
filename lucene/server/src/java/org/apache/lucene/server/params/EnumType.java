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

import java.util.Arrays;

public class EnumType extends Type {
  // value, desc, value, desc, ...:
  final String[] values;
  final String[] descriptions;

  public EnumType(String ... values) {
    if ((values.length & 1) != 0) {
      throw new IllegalArgumentException("input must be value/desc pairs");
    }
    this.values = new String[values.length/2];
    this.descriptions = new String[values.length/2];
    for(int i=0;i<values.length;i+=2) {
      this.values[i/2] = values[i];
      this.descriptions[i/2] = values[i+1];
    }
  }

  @Override
  public void validate(Object o) {
    if (!(o instanceof String)) {
      throw new IllegalArgumentException("expected String but got " + o.getClass());
    }
    for(int i=0;i<values.length;i++) {
      if (o.equals(values[i])) {
        return;
      }
    }
    throw new IllegalArgumentException("expected one of " + Arrays.toString(values) + " but got \"" + o + "\"");
  }
}
