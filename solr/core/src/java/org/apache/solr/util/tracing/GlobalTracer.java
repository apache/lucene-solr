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

package org.apache.solr.util.tracing;

import javax.servlet.http.HttpServletRequest;
import java.util.Random;

import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import io.opentracing.propagation.Format;

public class GlobalTracer {
  private static final Random RANDOM;
  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      RANDOM = new Random();
    } else {
      RANDOM = new Random(seed.hashCode());
    }
  }

  private static final Tracer NOOP_TRACER = NoopTracerFactory.create();
  private static volatile Tracer TRACER = NOOP_TRACER;
  private static volatile double rate;
  private final static ThreadLocal<Tracer> threadLocal = new ThreadLocal<>();

  public static void setup(Tracer tracer, double rate) {
    GlobalTracer.rate = rate / 100.0;
    GlobalTracer.TRACER = tracer;
  }

  public static boolean tracing() {
    return threadLocal.get() != null
        && threadLocal.get() != NOOP_TRACER
        && threadLocal.get().activeSpan() != null;
  }

  public static SpanContext extract(HttpServletRequest request) {
    SpanContext spanContext = TRACER.extract(Format.Builtin.HTTP_HEADERS, new HttpServletCarrier(request));
    if (spanContext != null) {
      threadLocal.set(TRACER);
    }
    return spanContext;
  }

  public static Tracer get() {
    Tracer tracer = threadLocal.get();
    if (tracer != null)
      return tracer;

    if (traced()) {
      threadLocal.set(TRACER);
    } else {
      threadLocal.set(NOOP_TRACER);
    }

    return threadLocal.get();
  }

  private static boolean traced() {
    return RANDOM.nextDouble() <= rate;
  }
}
