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

package org.apache.solr.cloud.api.collections;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.base.MoreObjects;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.TimeZoneUtils;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.params.CommonParams.TZ;

/**
 * Holds configuration for a routed alias, and some common code and constants.
 *
 * @see CreateAliasCmd
 * @see MaintainRoutedAliasCmd
 * @see org.apache.solr.update.processor.TimeRoutedAliasUpdateProcessor
 */
public class TimeRoutedAlias {

  // These are parameter names to routed alias creation, AND are stored as metadata with the alias.
  public static final String ROUTER_PREFIX = "router.";
  public static final String ROUTER_TYPE_NAME = ROUTER_PREFIX + "name";
  public static final String ROUTER_FIELD = ROUTER_PREFIX + "field";
  public static final String ROUTER_START = ROUTER_PREFIX + "start";
  public static final String ROUTER_INTERVAL = ROUTER_PREFIX + "interval";
  public static final String ROUTER_MAX_FUTURE = ROUTER_PREFIX + "maxFutureMs";
  public static final String ROUTER_PREEMPTIVE_CREATE_MATH = ROUTER_PREFIX + "preemptiveCreateMath";
  public static final String ROUTER_AUTO_DELETE_AGE = ROUTER_PREFIX + "autoDeleteAge";
  public static final String CREATE_COLLECTION_PREFIX = "create-collection.";
  // plus TZ and NAME

  /**
   * Parameters required for creating a routed alias
   */
  public static final List<String> REQUIRED_ROUTER_PARAMS = Collections.unmodifiableList(Arrays.asList(
      CommonParams.NAME,
      ROUTER_TYPE_NAME,
      ROUTER_FIELD,
      ROUTER_START,
      ROUTER_INTERVAL));

  /**
   * Optional parameters for creating a routed alias excluding parameters for collection creation.
   */
  //TODO lets find a way to remove this as it's harder to maintain than required list
  public static final List<String> OPTIONAL_ROUTER_PARAMS = Collections.unmodifiableList(Arrays.asList(
      ROUTER_MAX_FUTURE,
      ROUTER_AUTO_DELETE_AGE,
      ROUTER_PREEMPTIVE_CREATE_MATH,
      TZ)); // kinda special

  static Predicate<String> PARAM_IS_PROP =
      key -> key.equals(TZ) ||
          (key.startsWith(ROUTER_PREFIX) && !key.equals(ROUTER_START)) || //TODO reconsider START special case
          key.startsWith(CREATE_COLLECTION_PREFIX);

  public static final String ROUTED_ALIAS_NAME_CORE_PROP = "routedAliasName"; // core prop

  // This format must be compatible with collection name limitations
  private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE).appendPattern("[_HH[_mm[_ss]]]") //brackets mean optional
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter(Locale.ROOT).withZone(ZoneOffset.UTC); // deliberate -- collection names disregard TZ

  public static Instant parseInstantFromCollectionName(String aliasName, String collection) {
    final String dateTimePart = collection.substring(aliasName.length() + 1);
    return DATE_TIME_FORMATTER.parse(dateTimePart, Instant::from);
  }

  public static String formatCollectionNameFromInstant(String aliasName, Instant timestamp) {
    String nextCollName = DATE_TIME_FORMATTER.format(timestamp);
    for (int i = 0; i < 3; i++) { // chop off seconds, minutes, hours
      if (nextCollName.endsWith("_00")) {
        nextCollName = nextCollName.substring(0, nextCollName.length()-3);
      }
    }
    assert DATE_TIME_FORMATTER.parse(nextCollName, Instant::from).equals(timestamp);
    return aliasName + "_" + nextCollName;
  }


  //
  // Instance data and methods
  //

  private final String aliasName;
  private final String routeField;
  private final String intervalMath; // ex: +1DAY
  private final long maxFutureMs;
  private final String preemptiveCreateMath;
  private final String autoDeleteAgeMath; // ex: /DAY-30DAYS  *optional*
  private final TimeZone timeZone;

  public TimeRoutedAlias(String aliasName, Map<String, String> aliasMetadata) {
    this.aliasName = aliasName;
    final MapSolrParams params = new MapSolrParams(aliasMetadata); // for convenience
    final RequiredSolrParams required = params.required();
    if (!"time".equals(required.get(ROUTER_TYPE_NAME))) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Only 'time' routed aliases is supported right now.");
    }
    routeField = required.get(ROUTER_FIELD);
    intervalMath = required.get(ROUTER_INTERVAL);

    //optional:
    maxFutureMs = params.getLong(ROUTER_MAX_FUTURE, TimeUnit.MINUTES.toMillis(10));
    // the date math configured is an interval to be subtracted from the most recent collection's time stamp
    String pcmTmp = params.get(ROUTER_PREEMPTIVE_CREATE_MATH);
    preemptiveCreateMath = pcmTmp != null ? (pcmTmp.startsWith("-") ? pcmTmp : "-" + pcmTmp) : null;
    autoDeleteAgeMath = params.get(ROUTER_AUTO_DELETE_AGE); // no default
    timeZone = TimeZoneUtils.parseTimezone(aliasMetadata.get(CommonParams.TZ));

    // More validation:

    // check that the date math is valid
    final Date now = new Date();
    try {
      final Date after = new DateMathParser(now, timeZone).parseMath(intervalMath);
      if (!after.after(now)) {
        throw new SolrException(BAD_REQUEST, "duration must add to produce a time in the future");
      }
    } catch (Exception e) {
      throw new SolrException(BAD_REQUEST, "bad " + TimeRoutedAlias.ROUTER_INTERVAL + ", " + e, e);
    }

    if (autoDeleteAgeMath != null) {
      try {
        final Date before =  new DateMathParser(now, timeZone).parseMath(autoDeleteAgeMath);
        if (now.before(before)) {
          throw new SolrException(BAD_REQUEST, "duration must round or subtract to produce a time in the past");
        }
      } catch (Exception e) {
        throw new SolrException(BAD_REQUEST, "bad " + TimeRoutedAlias.ROUTER_AUTO_DELETE_AGE + ", " + e, e);
      }
    }
    if (preemptiveCreateMath != null) {
      try {
        new DateMathParser().parseMath(preemptiveCreateMath);
      } catch (ParseException e) {
        throw new SolrException(BAD_REQUEST, "Invalid date math for preemptiveCreateMath:" + preemptiveCreateMath);
      }
    }

    if (maxFutureMs < 0) {
      throw new SolrException(BAD_REQUEST, ROUTER_MAX_FUTURE + " must be >= 0");
    }
  }

  public String getAliasName() {
    return aliasName;
  }

  public String getRouteField() {
    return routeField;
  }

  public String getIntervalMath() {
    return intervalMath;
  }

  public long getMaxFutureMs() {
    return maxFutureMs;
  }

  public String getPreemptiveCreateWindow() {
    return preemptiveCreateMath;
  }

  public String getAutoDeleteAgeMath() {
    return autoDeleteAgeMath;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("aliasName", aliasName)
        .add("routeField", routeField)
        .add("intervalMath", intervalMath)
        .add("maxFutureMs", maxFutureMs)
        .add("preemptiveCreateMath", preemptiveCreateMath)
        .add("autoDeleteAgeMath", autoDeleteAgeMath)
        .add("timeZone", timeZone)
        .toString();
  }

  /** Parses the timestamp from the collection list and returns them in reverse sorted order (most recent 1st) */
  public List<Map.Entry<Instant,String>> parseCollections(Aliases aliases, Supplier<SolrException> aliasNotExist) {
    final List<String> collections = aliases.getCollectionAliasListMap().get(aliasName);
    if (collections == null) {
      throw aliasNotExist.get();
    }
    // note: I considered TreeMap but didn't like the log(N) just to grab the most recent when we use it later
    List<Map.Entry<Instant,String>> result = new ArrayList<>(collections.size());
    for (String collection : collections) {
      Instant colStartTime = parseInstantFromCollectionName(aliasName, collection);
      result.add(new AbstractMap.SimpleImmutableEntry<>(colStartTime, collection));
    }
    result.sort((e1, e2) -> e2.getKey().compareTo(e1.getKey())); // reverse sort by key
    return result;
  }

  /** Computes the timestamp of the next collection given the timestamp of the one before. */
  public Instant computeNextCollTimestamp(Instant fromTimestamp) {
    final Instant nextCollTimestamp =
        DateMathParser.parseMath(Date.from(fromTimestamp), "NOW" + intervalMath, timeZone).toInstant();
    assert nextCollTimestamp.isAfter(fromTimestamp);
    return nextCollTimestamp;
  }
}
