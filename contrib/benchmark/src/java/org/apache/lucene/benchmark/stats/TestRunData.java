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


import java.util.LinkedHashMap;
import java.util.Vector;
import java.util.Collection;
import java.util.Iterator;

/**
 * This class holds series of TimeData related to a single test run. TimeData
 * values may contribute to different measurements, so this class provides also
 * some useful methods to separate them.
 *
 */
public class TestRunData {
  private String id;

  /** Start and end time of this test run. */
  private long start = 0L, end = 0L;

  private LinkedHashMap data = new LinkedHashMap();

  public TestRunData() {}

  public TestRunData(String id) {
    this.id = id;
  }

    public LinkedHashMap getData()
    {
        return data;
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public long getEnd()
    {
        return end;
    }

    public long getStart()
    {
        return start;
    }

    /** Mark the starting time of this test run. */
  public void startRun() {
    start = System.currentTimeMillis();
  }

  /** Mark the ending time of this test run. */
  public void endRun() {
    end = System.currentTimeMillis();
  }

  /** Add a data point. */
  public void addData(TimeData td) {
    td.recordMemUsage();
    Vector v = (Vector) data.get(td.name);
    if (v == null) {
      v = new Vector();
      data.put(td.name, v);
    }
    v.add(td.clone());
  }

  /** Get a list of all available types of data points. */
  public Collection getLabels() {
    return data.keySet();
  }

  /** Get total values from all data points of a given type. */
  public TimeData getTotals(String label) {
    Vector v = (Vector) data.get(label);
      if (v == null)
      {
          return null;
      }
    TimeData res = new TimeData("TOTAL " + label);
    for (int i = 0; i < v.size(); i++) {
      TimeData td = (TimeData) v.get(i);
      res.count += td.count;
      res.elapsed += td.elapsed;
    }
    return res;
  }

  /** Get total values from all data points of all types.
   * @return a list of TimeData values for all types.
   */
  public Vector getTotals() {
    Collection labels = getLabels();
    Vector v = new Vector();
    Iterator it = labels.iterator();
    while (it.hasNext()) {
      TimeData td = getTotals((String) it.next());
      v.add(td);
    }
    return v;
  }

  /** Get memory usage stats for a given data type. */
  public MemUsage getMemUsage(String label) {
    Vector v = (Vector) data.get(label);
      if (v == null)
      {
          return null;
      }
    MemUsage res = new MemUsage();
    res.minFree = Long.MAX_VALUE;
    res.minTotal = Long.MAX_VALUE;
    long avgFree = 0L, avgTotal = 0L;
    for (int i = 0; i < v.size(); i++) {
      TimeData td = (TimeData) v.get(i);
        if (res.maxFree < td.freeMem)
        {
            res.maxFree = td.freeMem;
        }
        if (res.maxTotal < td.totalMem)
        {
            res.maxTotal = td.totalMem;
        }
        if (res.minFree > td.freeMem)
        {
            res.minFree = td.freeMem;
        }
        if (res.minTotal > td.totalMem)
        {
            res.minTotal = td.totalMem;
        }
      avgFree += td.freeMem;
      avgTotal += td.totalMem;
    }
    res.avgFree = avgFree / v.size();
    res.avgTotal = avgTotal / v.size();
    return res;
  }

  /** Return a string representation. */
  public String toString() {
    StringBuffer sb = new StringBuffer();
    Collection labels = getLabels();
    Iterator it = labels.iterator();
    while (it.hasNext()) {
      String label = (String) it.next();
        sb.append(id).append("-").append(label).append(" ").append(getTotals(label).toString(false)).append(" ");
        sb.append(getMemUsage(label).toScaledString(1024 * 1024, "MB")).append("\n");
    }
    return sb.toString();
  }
}
