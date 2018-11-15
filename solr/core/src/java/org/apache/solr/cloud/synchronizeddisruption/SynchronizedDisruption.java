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
import org.apache.solr.common.util.SuppressForbidden;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A disruption which runs on a scheduled using a {@link java.util.concurrent.ScheduledExecutorService} so it is executed
 * per the {@link org.apache.logging.log4j.core.util.CronExpression#getNextValidTimeAfter(Date)}.
 */
public abstract class SynchronizedDisruption implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private ScheduledExecutorService disruptionExecutor;
  @SuppressForbidden(reason = "Uses Log4J Class to handle Cron Expressions")
  private CronExpression cronExpression;
  private ScheduledFuture scheduledFuture;

  /**
   * @param executorService is a ScheduledExcutorService
   * @param cronExpression is a string cron expression based off of {@link org.apache.logging.log4j.core.util.CronExpression}
   * @throws ParseException contains details of issues with the input cron expression string
   */
  @SuppressForbidden(reason = "Uses Log4J Class to handle Cron Expressions")
  public SynchronizedDisruption(ScheduledExecutorService executorService, String cronExpression) throws ParseException {
    this.disruptionExecutor = executorService;
    this.cronExpression = new CronExpression(cronExpression);
  }

  /**
   * Prepares the disruption for the initial/next scheduled run
   */
  void prepare() {
    scheduledFuture = disruptionExecutor.schedule(this, getNextDelay(), TimeUnit.MILLISECONDS);
  }

  /**
   * @return The cron expression as a String
   */
  @SuppressForbidden(reason = "Uses Log4J Class to handle Cron Expressions")
  public String getCronSchedule() {
    return this.cronExpression.getCronExpression();
  }

  abstract void runDisruption();

  @Override
  public void run() {
    runDisruption();
    this.scheduledFuture = this.disruptionExecutor.schedule(this, getNextDelay(), TimeUnit.MILLISECONDS);
  }

  /** Prevents the disruption from running */
  void close() {
    if (scheduledFuture != null)
      scheduledFuture.cancel(false);
  }

  @SuppressForbidden(reason = "Uses Log4J Class to handle Cron Expressions")
  private long getNextDelay() {
    Date nextRunDate = this.cronExpression.getNextValidTimeAfter(new Date());
    long epochRun = nextRunDate.toInstant().toEpochMilli();
    long delay = epochRun - TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    log.info("Next Run Date: " + nextRunDate + ", next epoch to run:" + epochRun + ", delay: " + delay + "ms");
    return delay;
  }
}
