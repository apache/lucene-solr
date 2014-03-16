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

import java.util.List;

/** Type for a list of items of fixed subType. */
public class ListType extends Type {

  /** Type of our elements. */
  public final Type subType;

  /** Sole constructor. */
  public ListType(Type subType) {
    this.subType = subType;
  }

  @Override
  public void validate(Object _o) {
    if (!(_o instanceof List)) {
      throw new IllegalArgumentException("expected List but got " + _o.getClass());
    }
    for(Object o2 : (List) _o) {
      subType.validate(o2);
    }
  }

  /** Returns item subType. */
  public Type getSubType() {
    return subType;
  }
}
