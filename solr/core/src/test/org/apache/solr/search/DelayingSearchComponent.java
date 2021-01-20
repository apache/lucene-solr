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
package org.apache.solr.search;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;

/**
 * Search component used to add delay to each request.
 */
public class DelayingSearchComponent extends SearchComponent{

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    rb.rsp.addHttpHeader("Warning", "This is a test warning");
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    final long totalSleepMillis = rb.req.getParams().getLong("sleep",0);
    if (totalSleepMillis > 0) {
      final long totalSleepNanos = TimeUnit.NANOSECONDS.convert(totalSleepMillis, TimeUnit.MILLISECONDS);
      final long startNanos = System.nanoTime();
      try {
        // Thread.sleep() (and derivatives) are not garunteed to sleep the full amount:
        //   "subject to the precision and accuracy of system timers and schedulers."
        // This is particularly problematic on Windows VMs, so we do a retry loop
        // to ensure we sleep a total of at least as long as requested
        //
        // (Tests using this component do so explicitly to ensure 'timeAllowed'
        // has exceeded in order to get their expected results, we would rather over-sleep
        // then under sleep)
        for (long sleepNanos = totalSleepNanos;
             0 < sleepNanos;
             sleepNanos = totalSleepNanos - (System.nanoTime() - startNanos)) {
          TimeUnit.NANOSECONDS.sleep(sleepNanos);
        }
      } catch (InterruptedException e) {
        // Do nothing?
      }
    }
  }

  @Override
  public String getDescription() {
    return "SearchComponent used to add delay to each request";
  }

}
