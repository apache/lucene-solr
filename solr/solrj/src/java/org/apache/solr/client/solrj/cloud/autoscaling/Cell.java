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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.util.HashMap;

import org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;

/**Each instance represents an attribute that is being tracked by the framework such as , freedisk, cores etc
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class Cell implements MapWriter {
  final int index;
  final Type type;
  final String name;
  Object val, approxVal;
  Row row;

  public Cell(int index, String name, Object val, Object approxVal, Type type, Row row) {
    this.index = index;
    this.name = name;
    this.val = val;
    this.approxVal = approxVal;
    this.type = type;
    this.row = row;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put(name, val);
  }
  public Row getRow(){
    return row;
  }

  @Override
  public String toString() {
    return Utils.toJSONString(this.toMap(new HashMap<>()));
  }

  public Cell copy() {
    return new Cell(index, name, val, approxVal, this.type, row);
  }

  public String getName() {
    return name;
  }

  public Object getValue() {
    return val;
  }

  public Object getApproxValue() {
    return approxVal;
  }
}
