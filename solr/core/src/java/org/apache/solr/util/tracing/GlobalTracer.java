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

import com.google.common.annotations.VisibleForTesting;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import io.opentracing.propagation.Format;

public class GlobalTracer {
  private static final Tracer NOOP_TRACER = NoopTracerFactory.create();
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

  private static volatile GlobalTracer INS = new GlobalTracer(NOOP_TRACER);

  public synchronized static void setup(Tracer tracer) {
    if (INS != null) {
      INS.close();
    }
    INS = new GlobalTracer(tracer);
  }

  public static GlobalTracer get() {
    return INS;
  }

  public static Tracer getTracer() {
    return INS.tracer();
  }

  @VisibleForTesting
  final Tracer tracer;
  private double rate;
  private final ThreadLocal<Tracer> threadLocal = new ThreadLocal<>();

  public GlobalTracer(Tracer tracer) {
    this.tracer = tracer;
  }

  public synchronized void setSamplePercentage(double rate) {
    this.rate = rate / 100.0;
  }

  @VisibleForTesting
  public double getSampleRate() {
    return rate;
  }

  public boolean tracing() {
    return threadLocal.get() != null
        && threadLocal.get() != NOOP_TRACER
        && threadLocal.get().activeSpan() != null;
  }

  public SpanContext extract(HttpServletRequest request) {
    SpanContext spanContext = tracer.extract(Format.Builtin.HTTP_HEADERS, new HttpServletCarrier(request));
    if (spanContext != null) {
      threadLocal.set(tracer);
    }
    return spanContext;
  }

  private Tracer tracer() {
    Tracer tracer = threadLocal.get();
    if (tracer != null)
      return tracer;

    if (traced()) {
      threadLocal.set(this.tracer);
    } else {
      threadLocal.set(NOOP_TRACER);
    }

    return threadLocal.get();
  }

  /**
   * Clear tracing context for the current thread
   */
  public void clearContext() {
    threadLocal.remove();
  }

  private boolean traced() {
    return RANDOM.nextDouble() <= rate;
  }

  public void close() {
    tracer.close();
  }
}
