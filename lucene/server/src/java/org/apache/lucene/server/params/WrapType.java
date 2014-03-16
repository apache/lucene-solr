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

/** Wraps another type; use this to break type recursion. */
public class WrapType extends Type {
  private Type other;

  /** Sole constructor. */
  public WrapType() {
  }

  /** Set our wrapped type. */
  public void set(Type other) {
    if (this.other != null) {
      throw new IllegalStateException("already set");
    }

    this.other = other;
  }

  @Override
  public void validate(Object o) {
    other.validate(o);
  }

  /** Return the type we wrap. */
  public Type getWrappedType() {
    return other;
  }
}
