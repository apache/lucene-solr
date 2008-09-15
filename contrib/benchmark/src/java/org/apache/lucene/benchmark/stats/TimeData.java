package org.apache.lucene.benchmark.stats;
/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * This class holds a data point measuring speed of processing.
 *
 */
public class TimeData {
  /** Name of the data point - usually one of a data series with the same name */
  public String name;
  /** Number of records processed. */
  public long count = 0;
  /** Elapsed time in milliseconds. */
  public long elapsed = 0L;

  private long delta = 0L;
  /** Free memory at the end of measurement interval. */
  public long freeMem = 0L;
  /** Total memory at the end of measurement interval. */
  public long totalMem = 0L;

  public TimeData() {};

  public TimeData(String name) {
    this.name = name;
  }

  /** Start counting elapsed time. */
  public void start() {
    delta = System.currentTimeMillis();
  }

  /** Stop counting elapsed time. */
  public void stop() {
    count++;
    elapsed += (System.currentTimeMillis() - delta);
  }

  /** Record memory usage. */
  public void recordMemUsage() {
    freeMem = Runtime.getRuntime().freeMemory();
    totalMem = Runtime.getRuntime().totalMemory();
  }

  /** Reset counters. */
  public void reset() {
    count = 0;
    elapsed = 0L;
    delta = elapsed;
  }

  protected Object clone() {
    TimeData td = new TimeData(name);
    td.name = name;
    td.elapsed = elapsed;
    td.count = count;
    td.delta = delta;
    td.freeMem = freeMem;
    td.totalMem = totalMem;
    return td;
  }

  /** Get rate of processing, defined as number of processed records per second. */
  public double getRate() {
    double rps = (double) count * 1000.0 / (double) (elapsed>0 ? elapsed : 1); // assume atleast 1ms for any countable op
    return rps;
  }

  /** Get a short legend for toString() output. */
  public static String getLabels() {
    return "# count\telapsed\trec/s\tfreeMem\ttotalMem";
  }

  public String toString() { return toString(true); }
  /**
   * Return a tab-seprated string containing this data.
   * @param withMem if true, append also memory information
   * @return The String
   */
  public String toString(boolean withMem) {
    StringBuffer sb = new StringBuffer();
    sb.append(count + "\t" + elapsed + "\t" + getRate());
    if (withMem) sb.append("\t" + freeMem + "\t" + totalMem);
    return sb.toString();
  }
}
