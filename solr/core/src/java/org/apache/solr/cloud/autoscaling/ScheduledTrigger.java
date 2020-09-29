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

package org.apache.solr.cloud.autoscaling;

import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.TimeZoneUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.AutoScalingParams.PREFERRED_OP;

/**
 * A trigger which creates {@link TriggerEventType#SCHEDULED} events as per the configured schedule
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class ScheduledTrigger extends TriggerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DEFAULT_GRACE_DURATION = "+15MINUTES";
  private static final String LAST_RUN_AT = "lastRunAt";
  static final String ACTUAL_EVENT_TIME = "actualEventTime";

  private String everyStr;

  private String graceDurationStr;

  private String preferredOp;

  private TimeZone timeZone;

  private Instant lastRunAt;

  public ScheduledTrigger(String name) {
    super(TriggerEventType.SCHEDULED, name);
    TriggerUtils.requiredProperties(requiredProperties, validProperties, "startTime", "every");
    TriggerUtils.validProperties(validProperties, "timeZone", "graceDuration", AutoScalingParams.PREFERRED_OP);
  }

  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {
    super.configure(loader, cloudManager, properties);
    String timeZoneStr = (String) properties.get("timeZone");
    this.timeZone = TimeZoneUtils.parseTimezone(timeZoneStr); // defaults to UTC

    String startTimeStr = (String) properties.get("startTime");
    this.everyStr = (String) properties.get("every");
    this.graceDurationStr = (String) properties.getOrDefault("graceDuration", DEFAULT_GRACE_DURATION);

    preferredOp = (String) properties.get(PREFERRED_OP);
    if (preferredOp != null &&
        CollectionParams.CollectionAction.get(preferredOp) == null) {
      throw new TriggerValidationException(getName(), PREFERRED_OP, "unrecognized value of: '" + preferredOp + "'");
    }

    // attempt parsing to validate date math strings
    // explicitly set NOW because it may be different for simulated time
    Date now = new Date(TimeUnit.NANOSECONDS.toMillis(cloudManager.getTimeSource().getEpochTimeNs()));
    Instant startTime = parseStartTime(now, startTimeStr, timeZoneStr);
    DateMathParser.parseMath(now, startTime + everyStr, timeZone);
    DateMathParser.parseMath(now, startTime + graceDurationStr, timeZone);

    // We set lastRunAt to be the startTime (which could be a date math expression such as 'NOW')
    // Ordinarily, NOW will always be evaluated in this constructor so it may seem that
    // the trigger will always fire the first time.
    // However, the lastRunAt is overwritten with the value from ZK
    // during restoreState() operation (which is performed before run()) so the trigger works correctly
    this.lastRunAt = startTime;
  }

  private Instant parseStartTime(Date now, String startTimeStr, String timeZoneStr) throws TriggerValidationException {
    try {
      // try parsing startTime as an ISO-8601 date time string
      return DateMathParser.parseMath(now, startTimeStr).toInstant();
    } catch (SolrException e) {
      if (e.code() != SolrException.ErrorCode.BAD_REQUEST.code) {
        throw new TriggerValidationException("startTime", "error parsing value '" + startTimeStr + "': " + e.toString());
      }
    }
    if (timeZoneStr == null)  {
      throw new TriggerValidationException("timeZone",
          "Either 'startTime' should be an ISO-8601 date time string or 'timeZone' must be not be null");
    }
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneStr);
    DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_LOCAL_DATE).appendPattern("['T'[HH[:mm[:ss]]]]")
        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
        .toFormatter(Locale.ROOT).withZone(timeZone.toZoneId());
    try {
      return Instant.from(dateTimeFormatter.parse(startTimeStr));
    } catch (Exception e) {
      throw new TriggerValidationException("startTime", "error parsing startTime '" + startTimeStr + "': " + e.toString());
    }
  }

  @Override
  protected Map<String, Object> getState() {
    return Collections.singletonMap(LAST_RUN_AT, lastRunAt.toEpochMilli());
  }

  @Override
  protected void setState(Map<String, Object> state) {
    if (state.containsKey(LAST_RUN_AT)) {
      this.lastRunAt = Instant.ofEpochMilli((Long) state.get(LAST_RUN_AT));
    }
  }

  @Override
  public void restoreState(AutoScaling.Trigger old) {
    assert old.isClosed();
    if (old instanceof ScheduledTrigger) {
      ScheduledTrigger scheduledTrigger = (ScheduledTrigger) old;
      this.lastRunAt = scheduledTrigger.lastRunAt;
    } else  {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE,
          "Unable to restore state from an unknown type of trigger");
    }
  }

  @Override
  public void run() {
    synchronized (this) {
      if (isClosed) {
        log.debug("ScheduledTrigger ran but was already closed");
        return;
      }
    }

    TimeSource timeSource = cloudManager.getTimeSource();
    DateMathParser dateMathParser = new DateMathParser(timeZone);
    dateMathParser.setNow(new Date(lastRunAt.toEpochMilli()));
    Instant nextRunTime, nextPlusGrace;
    try {
      Date next = dateMathParser.parseMath(everyStr);
      dateMathParser.setNow(next);
      nextPlusGrace = dateMathParser.parseMath(graceDurationStr).toInstant();
      nextRunTime = next.toInstant();
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unable to calculate next run time. lastRan: " + lastRunAt.toString() + " and date math string: " + everyStr, e);
    }

    Instant now = Instant.ofEpochMilli(
        TimeUnit.NANOSECONDS.toMillis(timeSource.getEpochTimeNs()));
    AutoScaling.TriggerEventProcessor processor = processorRef.get();

    if (now.isBefore(nextRunTime)) {
      return; // it's not time yet
    }
    if (now.isAfter(nextPlusGrace)) {
      // we are past time and we could not run per schedule so skip this event
      if (log.isWarnEnabled())  {
        log.warn("ScheduledTrigger was not able to run event at scheduled time: {}. Now: {}",
            nextRunTime, now);
      }
      // Even though we are skipping the event, we need to notify any listeners of the IGNORED stage
      // so we create a dummy event with the ignored=true flag and ScheduledTriggers will do the rest
      if (processor != null && processor.process(new ScheduledEvent(getEventType(), getName(), timeSource.getTimeNs(),
          preferredOp, now.toEpochMilli(), true))) {
        lastRunAt = nextRunTime;
        return;
      }
    }

    if (processor != null)  {
      if (log.isDebugEnabled()) {
        log.debug("ScheduledTrigger {} firing registered processor for scheduled time {}, now={}", name,
            nextRunTime, now);
      }
      if (processor.process(new ScheduledEvent(getEventType(), getName(), timeSource.getTimeNs(),
          preferredOp, now.toEpochMilli()))) {
        lastRunAt = nextRunTime; // set to nextRunTime instead of now to avoid drift
      }
    } else  {
      lastRunAt = nextRunTime; // set to nextRunTime instead of now to avoid drift
    }
  }

  public static class ScheduledEvent extends TriggerEvent {
    public ScheduledEvent(TriggerEventType eventType, String source, long eventTime, String preferredOp, long actualEventTime) {
      this(eventType, source, eventTime, preferredOp, actualEventTime, false);
    }

    public ScheduledEvent(TriggerEventType eventType, String source, long eventTime, String preferredOp, long actualEventTime, boolean ignored) {
      super(eventType, source, eventTime, null, ignored);
      if (preferredOp != null)  {
        properties.put(PREFERRED_OP, preferredOp);
      }
      properties.put(ACTUAL_EVENT_TIME, actualEventTime);
    }
  }
}
