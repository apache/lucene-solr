package org.apache.solr.util;

import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.StringHelper;

/**
 * Helper class for generating unique ID-s.
 */
public class IdUtils {

  /**
   * Generate a short random id (see {@link StringHelper#randomId()}).
   */
  public static final String randomId() {
    return StringHelper.idToString(StringHelper.randomId());
  }

  /**
   * Generate a random id with a timestamp, in the format:
   * <code>hex(timestamp) + 'T' + randomId</code>. This method
   * uses {@link TimeSource#CURRENT_TIME} for timestamp values.
   */
  public static final String timeRandomId() {
    return timeRandomId(TimeUnit.MILLISECONDS.convert(TimeSource.CURRENT_TIME.getTime(), TimeUnit.NANOSECONDS));
  }

  /**
   * Generate a random id with a timestamp, in the format:
   * <code>hex(timestamp) + 'T' + randomId</code>.
   * @param time value representing timestamp
   */
  public static final String timeRandomId(long time) {
    StringBuilder sb = new StringBuilder(Long.toHexString(time));
    sb.append('T');
    sb.append(randomId());
    return sb.toString();
  }
}
