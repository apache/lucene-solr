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

/** Represents one paramater to a method. */
public class Param {

  /** Name. */
  public final String name;

  /** Description (English). */
  public final String desc;

  /** Type description. */
  public final Type type;

  /** Default value; if null the parameter is required. */
  public final Object defaultValue;

  /** Creates this, with a default value. */
  public Param(String name, String desc, Type type, Object defaultValue) {
    this.name = name;
    this.desc = desc;
    this.type = type;
    this.defaultValue = defaultValue;
    if (defaultValue != null) {
      type.validate(defaultValue);
    }
  }

  /** Creates this, with no default value. */
  public Param(String name, String desc, Type type) {
    this(name, desc, type, null);
  }
}
