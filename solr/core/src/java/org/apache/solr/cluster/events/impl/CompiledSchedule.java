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
package org.apache.solr.cluster.events.impl;

import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.solr.cluster.events.Schedule;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.TimeZoneUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A version of {@link Schedule} where some of the fields are already resolved.
 */
class CompiledSchedule {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final String name;
  final TimeZone timeZone;
  final Instant startTime;
  final String interval;
  final DateMathParser dateMathParser;

  Instant lastRunAt;

  /**
   * Compile a schedule.
   * @param schedule schedule.
   * @throws Exception if startTime or interval cannot be parsed.
   */
  CompiledSchedule(Schedule schedule) throws Exception {
    this.name = schedule.getName();
    this.timeZone = TimeZoneUtils.getTimeZone(schedule.getTimeZone());
    this.startTime = parseStartTime(new Date(), schedule.getStartTime(), timeZone);
    this.lastRunAt = startTime;
    this.interval = schedule.getInterval();
    this.dateMathParser = new DateMathParser(timeZone);
    // this is just to verify that the interval math is valid
    shouldRun();
  }

  private Instant parseStartTime(Date now, String startTimeStr, TimeZone timeZone) throws Exception {
    try {
      // try parsing startTime as an ISO-8601 date time string
      return DateMathParser.parseMath(now, startTimeStr).toInstant();
    } catch (SolrException e) {
      if (e.code() != SolrException.ErrorCode.BAD_REQUEST.code) {
        throw new Exception("startTime: error parsing value '" + startTimeStr + "': " + e.toString());
      }
    }
    DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_LOCAL_DATE).appendPattern("['T'[HH[:mm[:ss]]]]")
        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
        .toFormatter(Locale.ROOT).withZone(timeZone.toZoneId());
    try {
      return Instant.from(dateTimeFormatter.parse(startTimeStr));
    } catch (Exception e) {
      throw new Exception("startTime: error parsing startTime '" + startTimeStr + "': " + e.toString());
    }
  }

  /**
   * Returns true if the last run + run interval is already in the past.
   */
  boolean shouldRun() {
    dateMathParser.setNow(new Date(lastRunAt.toEpochMilli()));
    Instant nextRunTime;
    try {
      Date next = dateMathParser.parseMath(interval);
      nextRunTime = next.toInstant();
    } catch (ParseException e) {
      log.warn("Invalid math expression, skipping: " + e);
      return false;
    }
    if (Instant.now().isAfter(nextRunTime)) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * This setter MUST be invoked after each run.
   * @param lastRunAt time when the schedule was last run.
   */
  void setLastRunAt(Instant lastRunAt) {
    this.lastRunAt = lastRunAt;
  }
}
