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

import java.time.Instant;
import java.util.Date;
import java.util.function.Consumer;

/**
 * A multi-valued analytics value that can be represented as a date.
 * <p>
 * The back-end production of the value can change inbetween calls to {@link #streamDates},
 * resulting in different values on each call.
 * <p>
 * NOTE: Most date expressions work with the {@code long} representation of the date, so that less
 * objects need to be created during execution.
 */
public interface DateValueStream extends LongValueStream {
  /**
   * Stream the date representations of all current values, if any exist.
   *
   * @param cons The consumer to accept the values
   */
  void streamDates(Consumer<Date> cons);

  /**
   * An interface that represents all of the types a {@link DateValueStream} should be able to cast to.
   */
  public static interface CastingDateValueStream extends DateValueStream, LongValueStream, StringValueStream {}

  /**
   * An abstract base for {@link CastingDateValueStream} that automatically casts to all types if {@link #streamLongs} is implemented.
   */
  public static abstract class AbstractDateValueStream implements CastingDateValueStream {
    @Override
    public void streamDates(Consumer<Date> cons) {
      streamLongs((long val) -> cons.accept(new Date(val)));
    }
    @Override
    public void streamStrings(Consumer<String> cons) {
      streamLongs((long val) -> cons.accept(Instant.ofEpochMilli(val).toString()));
    }
    @Override
    public void streamObjects(Consumer<Object> cons) {
      streamDates((Date val) -> cons.accept(val));
    }
    @Override
    public AnalyticsValueStream convertToConstant() {
      return this;
    }
  }
}
