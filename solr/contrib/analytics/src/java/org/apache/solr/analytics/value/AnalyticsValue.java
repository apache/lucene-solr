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
/**
 * A single-valued analytics value.
 * <p>
 * The back-end production of the value can change inbetween calls to {@link #getObject()} and {@link #exists()},
 * resulting in different values on each call.
 */
public interface AnalyticsValue extends AnalyticsValueStream {
  /**
   * Check whether the current value exists.
   * <br>
   * NOTE: The result of this method is only guaranteed after any {@code get<Type>()} method is called.
   *
   * @return whether the current value exists
   */
  boolean exists();

  /**
   * Get the object representation of the current value.
   *
   * @return the current value
   */
  Object getObject();

  /**
   * An abstract base for {@link AnalyticsValue} that automatically casts to castable types.
   */
  public static abstract class AbstractAnalyticsValue implements AnalyticsValue {
    @Override
    public void streamObjects(Consumer<Object> cons) {
      Object value = getObject();
      if (exists()) {
        cons.accept(value);
      }
    }
    @Override
    public AnalyticsValueStream convertToConstant() {
      return this;
    }
  }
}
