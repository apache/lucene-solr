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
package org.apache.solr.client.solrj.io.eval;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

import java.util.Iterator;

public class Matrix implements Iterable {

  private double[][] data;
  private Map context = new HashMap();

  public Matrix(double[][] data) {
    this.data = data;
  }

  public Map getContext() {
    return this.context;
  }

  public void addToContext(Object key, Object value) {
    this.context.put(key, value);
  }

  public Object getContextValue(Object key) {
    return this.context.get(key);
  }

  public double[][] getData() {
    return this.data;
  }

  public Iterator iterator() {
    return new MatrixIterator(data);
  }

  private static class MatrixIterator implements Iterator {

    private double[][] d;
    private int index;

    public MatrixIterator(double[][] data) {
      d = data;
    }

    public Object next() {
      double[] row = d[index++];
      List list = new ArrayList();
      for(double value : row) {
        list.add(value);
      }

      return list;
    }

    public boolean hasNext() {
      return index < d.length;
    }
  }
}
