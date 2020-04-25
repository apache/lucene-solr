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
package org.apache.solr.util;

import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;

/**
 * Simple utilities for working with TimeZones
 * @see java.util.TimeZone
 */
public final class TimeZoneUtils {

  private TimeZoneUtils() {
    // :NOOP:
  }

  /**
   * An immutable Set of all TimeZone IDs supported by the TimeZone class 
   * at the moment the TimeZoneUtils was initialized.
   * 
   * @see TimeZone#getAvailableIDs
   */
  public static final Set<String> KNOWN_TIMEZONE_IDS 
    = Set.of(TimeZone.getAvailableIDs());

  /**
   * This method is provided as a replacement for TimeZone.getTimeZone but 
   * without the annoying behavior of returning "GMT" for gibberish input.
   * <p>
   * This method will return null unless the input is either:
   * </p>
   * <ul>
   *  <li>Included in the set of known TimeZone IDs</li>
   *  <li>A "CustomID" specified as a numeric offset from "GMT"</li>
   * </ul>
   * 
   * @param ID Either a TimeZone ID found in KNOWN_TIMEZONE_IDS, or a "CustomID" specified as a GMT offset.
   * @return A TimeZone object corresponding to the input, or null if no such TimeZone is supported.
   * @see #KNOWN_TIMEZONE_IDS
   * @see TimeZone
   */
  public static final TimeZone getTimeZone(final String ID) {
    if (null == ID) return null;
    if (KNOWN_TIMEZONE_IDS.contains(ID)) return TimeZone.getTimeZone(ID);

    Matcher matcher = CUSTOM_ID_REGEX.matcher(ID);
    if (matcher.matches()) {
      int hour = Integer.parseInt(matcher.group(1));
      if (hour < 0 || 23 < hour) return null;
      
      final String minStr = matcher.group(2);
      if (null != minStr) {
        int min = Integer.parseInt(minStr);
        if (min < 0 || 59 < min) return null;
      }
      return TimeZone.getTimeZone(ID);
    }
    return null;
  }

  private static Pattern CUSTOM_ID_REGEX = Pattern.compile("GMT(?:\\+|\\-)(\\d{1,2})(?::?(\\d{2}))?");

  /**
   * Parse the specified timezone ID. If null input then return UTC. If we can't resolve it then
   * throw an exception.  Does not return null.
   */
  public static TimeZone parseTimezone(String tzStr) {
    if (tzStr != null) {
      TimeZone tz = getTimeZone(tzStr);
      if (null == tz) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Solr JVM does not support TZ: " + tzStr);
      }
      return tz;
    } else {
      return DateMathParser.UTC; //TODO move to TimeZoneUtils
    }
  }
}
