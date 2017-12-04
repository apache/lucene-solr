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
package org.apache.solr.analytics.value;

import java.util.function.Consumer;

import org.apache.solr.analytics.util.function.BooleanConsumer;

/**
 * A multi-valued analytics value that can be represented as a boolean.
 * <p>
 * The back-end production of the value can change inbetween calls to {@link #streamBooleans},
 * resulting in different values on each call.
 */
public interface BooleanValueStream extends AnalyticsValueStream {
  /**
   * Stream the boolean representations of all current values, if any exist.
   * 
   * @param cons The consumer to accept the values
   */
  void streamBooleans(BooleanConsumer cons);
  
  /**
   * An interface that represents all of the types a {@link BooleanValueStream} should be able to cast to. 
   */
  public static interface CastingBooleanValueStream extends BooleanValueStream, StringValueStream {}
  
  /**
   * An abstract base for {@link CastingBooleanValueStream} that automatically casts to all types if {@link #streamBooleans} is implemented.
   */
  public static abstract class AbstractBooleanValueStream implements CastingBooleanValueStream {
    @Override
    public void streamStrings(Consumer<String> cons) {
      streamBooleans((boolean val) -> cons.accept(Boolean.toString(val)));
    }
    @Override
    public void streamObjects(Consumer<Object> cons) {
      streamBooleans((boolean val) -> cons.accept(val));
    }
    @Override
    public AnalyticsValueStream convertToConstant() {
      return this;
    }
  }
}
