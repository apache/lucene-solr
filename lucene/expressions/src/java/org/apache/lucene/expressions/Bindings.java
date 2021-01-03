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
package org.apache.lucene.expressions;

import org.apache.lucene.search.DoubleValuesSource;

/**
 * Binds variable names in expressions to actual data.
 *
 * <p>These are typically DocValues fields/FieldCache, the document's relevance score, or other
 * ValueSources.
 *
 * @lucene.experimental
 */
public abstract class Bindings {

  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected Bindings() {}

  /** Returns a DoubleValuesSource bound to the variable name */
  public abstract DoubleValuesSource getDoubleValuesSource(String name);
}
