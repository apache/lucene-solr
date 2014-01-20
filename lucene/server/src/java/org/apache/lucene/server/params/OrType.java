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

/** Type that accepts any of the provided list of types. */
public class OrType extends Type {

  /** Allowed sub-types. */
  public final Type[] types;

  /** Sole constructor. */
  public OrType(Type... types) {
    this.types = types;
  }

  @Override
  public void validate(Object o) {
    for(Type t : types) {
      try {
        t.validate(o);
        return;
      } catch (IllegalArgumentException iae) {
      }
    }
    StringBuilder sb = new StringBuilder();
    sb.append("expected one of ");
    for(int i=0;i<types.length;i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(types[i].getClass().getSimpleName());
    }
    sb.append(", but got " + o.getClass().getSimpleName());

    throw new IllegalArgumentException(sb.toString());
  }
}
