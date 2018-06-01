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

package org.apache.solr.cloud.synchronizeddisruption;

import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.core.util.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SynchronizedDisruption implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private ScheduledExecutorService disruptionExecutor;
  private CronExpression cronExpression;
  private ScheduledFuture scheduledFuture;

  public SynchronizedDisruption(ScheduledExecutorService executorService, String cronExpression) throws ParseException {
    this.disruptionExecutor = executorService;
    this.cronExpression = new CronExpression(cronExpression);
  }

  void prepare() {
    scheduledFuture = disruptionExecutor.schedule(this, getNextDelay(), TimeUnit.MILLISECONDS);
  }

  abstract void runDisruption();

  @Override
  public void run() {
    runDisruption();
    this.scheduledFuture = this.disruptionExecutor.schedule(this, getNextDelay(), TimeUnit.MILLISECONDS);
  }

  void close() {
    if (scheduledFuture != null)
      scheduledFuture.cancel(false);
  }

  private long getNextDelay() {
    Date nextRunDate = this.cronExpression.getNextValidTimeAfter(new Date());
    long epochRun = nextRunDate.toInstant().toEpochMilli();
    long delay = epochRun - System.currentTimeMillis();
    logger.info("Next Run Date: " + nextRunDate + ", next epoch to run:" + epochRun + ", delay: " + delay + "ms");
    return delay;
  }
}
