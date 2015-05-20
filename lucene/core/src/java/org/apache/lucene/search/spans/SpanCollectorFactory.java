package org.apache.lucene.search.spans;

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

/**
 * Interface defining a factory for creating new {@link SpanCollector}s
 * @param <T> the SpanCollector type
 */
public interface SpanCollectorFactory<T extends SpanCollector> {

  /**
   * @return a new SpanCollector
   */
  T newCollector();

  /**
   * Factory for creating NO_OP collectors
   */
  public static final SpanCollectorFactory<?> NO_OP_FACTORY = new SpanCollectorFactory() {
    @Override
    public SpanCollector newCollector() {
      return SpanCollector.NO_OP;
    }
  };

}
