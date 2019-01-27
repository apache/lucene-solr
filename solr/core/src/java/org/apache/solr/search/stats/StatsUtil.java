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

import java.lang.invoke.MethodHandles;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various utilities for de/serialization of term stats and collection stats.
 */
public class StatsUtil {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  /**
   * Make a String representation of {@link CollectionStats}
   */
  public static String colStatsToString(CollectionStats colStats) {
    StringBuilder sb = new StringBuilder();
    sb.append(colStats.field);
    sb.append(',');
    sb.append(String.valueOf(colStats.maxDoc));
    sb.append(',');
    sb.append(String.valueOf(colStats.docCount));
    sb.append(',');
    sb.append(String.valueOf(colStats.sumTotalTermFreq));
    sb.append(',');
    sb.append(String.valueOf(colStats.sumDocFreq));
    return sb.toString();
  }
  
  private static CollectionStats colStatsFromString(String data) {
    if (data == null || data.trim().length() == 0) {
      log.warn("Invalid empty collection stats string");
      return null;
    }
    String[] vals = data.split(",");
    if (vals.length != 5) {
      log.warn("Invalid collection stats string, num fields " + vals.length
          + " != 5, '" + data + "'");
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
      log.warn("Invalid collection stats string '" + data + "': "
          + e.toString());
      return null;
    }
  }
  
  public static String termToString(Term t) {
    StringBuilder sb = new StringBuilder();
    sb.append(t.field()).append(':');
    BytesRef bytes = t.bytes();
    sb.append(Base64.byteArrayToBase64(bytes.bytes, bytes.offset, bytes.offset));
    return sb.toString();
  }
  
  private static Term termFromString(String data) {
    if (data == null || data.trim().length() == 0) {
      log.warn("Invalid empty term value");
      return null;
    }
    int idx = data.indexOf(':');
    if (idx == -1) {
      log.warn("Invalid term data without ':': '" + data + "'");
      return null;
    }
    String field = data.substring(0, idx);
    String value = data.substring(idx + 1);
    try {
      return new Term(field, value);
      // XXX this would be more correct
      // byte[] bytes = Base64.base64ToByteArray(value);
      // return new Term(field, new BytesRef(bytes));
    } catch (Exception e) {
      log.warn("Invalid term value '" + value + "'");
      return null;
    }
  }
  
  public static String termStatsToString(TermStats termStats,
      boolean includeTerm) {
    StringBuilder sb = new StringBuilder();
    if (includeTerm) {
      sb.append(termStats.term).append(',');
    }
    sb.append(String.valueOf(termStats.docFreq));
    sb.append(',');
    sb.append(String.valueOf(termStats.totalTermFreq));
    return sb.toString();
  }
  
  private static TermStats termStatsFromString(String data, Term t) {
    if (data == null || data.trim().length() == 0) {
      log.warn("Invalid empty term stats string");
      return null;
    }
    String[] vals = data.split(",");
    if (vals.length < 2) {
      log.warn("Invalid term stats string, num fields " + vals.length
          + " < 2, '" + data + "'");
      return null;
    }
    Term termToUse;
    int idx = 0;
    if (vals.length == 3) {
      idx++;
      // with term
      Term term = termFromString(vals[0]);
      if (term != null) {
        termToUse = term;
        if (t != null) {
          assert term.equals(t);
        }
      } else { // failed term decoding
        termToUse = t;
      }
    } else {
      termToUse = t;
    }
    if (termToUse == null) {
      log.warn("Missing term in termStats '" + data + "'");
      return null;
    }
    try {
      long docFreq = Long.parseLong(vals[idx++]);
      long totalTermFreq = Long.parseLong(vals[idx]);
      return new TermStats(termToUse.toString(), docFreq, totalTermFreq);
    } catch (Exception e) {
      log.warn("Invalid termStats string '" + data + "'");
      return null;
    }
  }
  
  public static Map<String,CollectionStats> colStatsMapFromString(String data) {
    if (data == null || data.trim().length() == 0) {
      return null;
    }
    Map<String,CollectionStats> map = new HashMap<String,CollectionStats>();
    String[] entries = data.split("!");
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
        sb.append('!');
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
    String[] entries = data.split("!");
    for (String es : entries) {
      TermStats termStats = termStatsFromString(es, null);
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
        sb.append('!');
      }
      sb.append(termStatsToString(e.getValue(), true));
    }
    return sb.toString();
  }
  
}
