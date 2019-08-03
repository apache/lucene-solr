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
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.util.function.FloatConsumer;

/**
 * A multi-valued analytics value that can be represented as a int.
 * <p>
 * The back-end production of the value can change inbetween calls to {@link #streamInts},
 * resulting in different values on each call.
 */
public interface IntValueStream extends AnalyticsValueStream {
  /**
   * Stream the int representations of all current values, if any exist.
   *
   * @param cons The consumer to accept the values
   */
  void streamInts(IntConsumer cons);

  /**
   * An interface that represents all of the types a {@link IntValueStream} should be able to cast to.
   */
  public static interface CastingIntValueStream extends IntValueStream, LongValueStream, FloatValueStream,
                                                        DoubleValueStream, StringValueStream {}

  /**
   * An abstract base for {@link CastingIntValueStream} that automatically casts to all types if {@link #streamInts} is implemented.
   */
  public static abstract class AbstractIntValueStream implements CastingIntValueStream {
    @Override
    public void streamLongs(LongConsumer cons) {
      streamInts((int val) -> cons.accept(val));
    }
    @Override
    public void streamFloats(FloatConsumer cons) {
      streamInts((int val) -> cons.accept(val));
    }
    @Override
    public void streamDoubles(DoubleConsumer cons) {
      streamInts((int val) -> cons.accept(val));
    }
    @Override
    public void streamStrings(Consumer<String> cons) {
      streamInts((int val) -> cons.accept(Integer.toString(val)));
    }
    @Override
    public void streamObjects(Consumer<Object> cons) {
      streamInts((int val) -> cons.accept(val));
    }
    @Override
    public AnalyticsValueStream convertToConstant() {
      return this;
    }
  }
}
