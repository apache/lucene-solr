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
package org.apache.solr.search.stats;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various utilities for de/serialization of term stats and collection stats.
 * <p>TODO: serialization format is very simple and does nothing to compress the data.</p>
 */
public class StatsUtil {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ENTRY_SEPARATOR = "!";
  public static final char ENTRY_SEPARATOR_CHAR = '!';

  /**
   * Parse a list of urls separated by "|" in order to retrieve a shard name.
   * @param collectionName collection name
   * @param shardUrls list of urls
   * @return shard name, or shardUrl if no shard info is present,
   *        or null if impossible to determine (eg. empty string)
   */
  public static String shardUrlToShard(String collectionName, String shardUrls) {
    // we may get multiple replica urls
    String[] urls = shardUrls.split("\\|");
    if (urls.length == 0) {
      return null;
    }
    String[] urlParts = urls[0].split("/");
    String coreName = urlParts[urlParts.length - 1];
    String replicaName = Utils.parseMetricsReplicaName(collectionName, coreName);
    String shard;
    if (replicaName != null) {
      shard = coreName.substring(collectionName.length() + 1);
      shard = shard.substring(0, shard.length() - replicaName.length() - 1);
    } else {
      if (coreName.length() > collectionName.length() && coreName.startsWith(collectionName)) {
        shard = coreName.substring(collectionName.length() + 1);
        if (shard.isEmpty()) {
          shard = urls[0];
        }
      } else {
        shard = urls[0];
      }
    }
    return shard;
  }

  public static String termsToEncodedString(Collection<?> terms) {
    StringBuilder sb = new StringBuilder();
    for (Object o : terms) {
      if (sb.length() > 0) {
        sb.append(ENTRY_SEPARATOR);
      }
      if (o instanceof Term) {
        sb.append(termToEncodedString((Term) o));
      } else {
        sb.append(termToEncodedString(String.valueOf(o)));
      }
    }
    return sb.toString();
  }

  public static Set<Term> termsFromEncodedString(String data) {
    Set<Term> terms = new HashSet<>();
    if (data == null || data.trim().isEmpty()) {
      return terms;
    }
    String[] items = data.split(ENTRY_SEPARATOR);
    for (String item : items) {
      Term t = termFromEncodedString(item);
      if (t != null) {
        terms.add(t);
      }
    }
    return terms;
  }

  public static Set<String> fieldsFromString(String data) {
    Set<String> fields = new HashSet<>();
    if (data == null || data.trim().isEmpty()) {
      return fields;
    }
    String[] items = data.split(ENTRY_SEPARATOR);
    for (String item : items) {
      if (!item.trim().isEmpty()) {
        fields.add(item);
      }
    }
    return fields;
  }

  public static String fieldsToString(Collection<String> fields) {
    StringBuilder sb = new StringBuilder();
    for (String field : fields) {
      if (field.trim().isEmpty()) {
        continue;
      }
      if (sb.length() > 0) {
        sb.append(ENTRY_SEPARATOR);
      }
      sb.append(field);
    }
    return sb.toString();
  }

  /**
   * Make a String representation of {@link CollectionStats}
   */
  public static String colStatsToString(CollectionStats colStats) {
    StringBuilder sb = new StringBuilder();
    sb.append(colStats.field);
    sb.append(',');
    sb.append(colStats.maxDoc);
    sb.append(',');
    sb.append(colStats.docCount);
    sb.append(',');
    sb.append(colStats.sumTotalTermFreq);
    sb.append(',');
    sb.append(colStats.sumDocFreq);
    return sb.toString();
  }
  
  private static CollectionStats colStatsFromString(String data) {
    if (data == null || data.trim().length() == 0) {
      log.warn("Invalid empty collection stats string");
      return null;
    }
    String[] vals = data.split(",");
    if (vals.length != 5) {
      log.warn("Invalid collection stats string, num fields {} != 5 '{}'", vals.length, data);
      return null;
    }
    String field = vals[0];
    try {
      long maxDoc = Long.parseLong(vals[1]);
      long docCount = Long.parseLong(vals[2]);
      long sumTotalTermFreq = Long.parseLong(vals[3]);
      long sumDocFreq = Long.parseLong(vals[4]);
      return new CollectionStats(field, maxDoc, docCount, sumTotalTermFreq,
          sumDocFreq);
    } catch (Exception e) {
      log.warn("Invalid collection stats string '{}', ", data, e);
      return null;
    }
  }
  
  public static String termToEncodedString(Term t) {
    StringBuilder sb = new StringBuilder();
    sb.append(t.field()).append(':');
    sb.append(encode(t.text()));
    return sb.toString();
  }

  public static final char ESCAPE = '_';
  public static final char ESCAPE_ENTRY_SEPARATOR = '0';

  public static String encode(String value) {
    StringBuilder output = new StringBuilder(value.length() + 2);
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case ESCAPE :
          output.append(ESCAPE).append(ESCAPE);
          break;
        case ENTRY_SEPARATOR_CHAR :
          output.append(ESCAPE).append(ESCAPE_ENTRY_SEPARATOR);
          break;
        default :
          output.append(c);
      }
    }
    try {
      return URLEncoder.encode(output.toString(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Apparently your JVM doesn't support UTF-8 encoding?", e);
    }
  }

  public static String decode(String value) throws IOException {
    value = URLDecoder.decode(value, "UTF-8");
    StringBuilder output = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      // escaped char follows
      if (c == ESCAPE && i < value.length() - 1) {
        i++;
        char next = value.charAt(i);
        if (next == ESCAPE) {
          output.append(ESCAPE);
        } else if (next == ESCAPE_ENTRY_SEPARATOR) {
          output.append(ENTRY_SEPARATOR_CHAR);
        } else {
          throw new IOException("invalid escape sequence in " + value);
        }
      } else {
        output.append(c);
      }
    }
    return output.toString();
  }

  public static String termToEncodedString(String term) {
    int idx = term.indexOf(':');
    if (idx == -1) {
      log.warn("Invalid term data without ':': '{}'", term);
      return null;
    }
    String prefix = term.substring(0, idx + 1);
    String value = term.substring(idx + 1);
    return prefix + encode(value);
  }
  
  public static Term termFromEncodedString(String data) {
    if (data == null || data.trim().length() == 0) {
      log.warn("Invalid empty term value");
      return null;
    }
    int idx = data.indexOf(':');
    if (idx == -1) {
      log.warn("Invalid term data without ':': '{}'", data);
      return null;
    }
    String field = data.substring(0, idx);
    String value = data.substring(idx + 1);
    try {
       return new Term(field, decode(value));
    } catch (Exception e) {
      log.warn("Invalid term value '{}'", value);
      return null;
    }
  }
  
  public static String termStatsToString(TermStats termStats, boolean encode) {
    StringBuilder sb = new StringBuilder();
    sb.append(encode ? termToEncodedString(termStats.term) : termStats.term).append(',');
    sb.append(termStats.docFreq);
    sb.append(',');
    sb.append(termStats.totalTermFreq);
    return sb.toString();
  }
  
  private static TermStats termStatsFromString(String data) {
    if (data == null || data.trim().length() == 0) {
      log.warn("Invalid empty term stats string");
      return null;
    }
    String[] vals = data.split(",");
    if (vals.length < 3) {
      log.warn("Invalid term stats string, num fields {} < 3, '{}'", vals.length, data);
      return null;
    }
    Term term = termFromEncodedString(vals[0]);
    try {
      long docFreq = Long.parseLong(vals[1]);
      long totalTermFreq = Long.parseLong(vals[2]);
      return new TermStats(term.toString(), docFreq, totalTermFreq);
    } catch (Exception e) {
      log.warn("Invalid termStats string '{}'", data);
      return null;
    }
  }

  public static Map<String,CollectionStats> colStatsMapFromString(String data) {
    if (data == null || data.trim().length() == 0) {
      return null;
    }
    Map<String,CollectionStats> map = new HashMap<String,CollectionStats>();
    String[] entries = data.split(ENTRY_SEPARATOR);
    for (String es : entries) {
      CollectionStats stats = colStatsFromString(es);
      if (stats != null) {
        map.put(stats.field, stats);
      }
    }
    return map;
  }
  
  public static String colStatsMapToString(Map<String,CollectionStats> stats) {
    if (stats == null || stats.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (Entry<String,CollectionStats> e : stats.entrySet()) {
      if (sb.length() > 0) {
        sb.append(ENTRY_SEPARATOR);
      }
      sb.append(colStatsToString(e.getValue()));
    }
    return sb.toString();
  }
  
  public static Map<String,TermStats> termStatsMapFromString(String data) {
    if (data == null || data.trim().length() == 0) {
      return null;
    }
    Map<String,TermStats> map = new HashMap<>();
    String[] entries = data.split(ENTRY_SEPARATOR);
    for (String es : entries) {
      TermStats termStats = termStatsFromString(es);
      if (termStats != null) {
        map.put(termStats.term, termStats);
      }
    }
    return map;
  }
  
  public static String termStatsMapToString(Map<String,TermStats> stats) {
    if (stats == null || stats.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (Entry<String,TermStats> e : stats.entrySet()) {
      if (sb.length() > 0) {
        sb.append(ENTRY_SEPARATOR);
      }
      sb.append(termStatsToString(e.getValue(), true));
    }
    return sb.toString();
  }
  
}
