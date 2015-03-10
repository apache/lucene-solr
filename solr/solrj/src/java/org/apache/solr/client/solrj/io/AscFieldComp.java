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

package org.apache.solr.client.solrj.io;

import java.io.Serializable;
import java.util.Comparator;

public class AscFieldComp implements Comparator<Tuple>, Serializable {

  private static final long serialVersionUID = 1;

  private String field;

  public AscFieldComp(String field) {
    this.field = field;
  }

  public int compare(Tuple t1, Tuple t2) {
    Comparable o1 = (Comparable)t1.get(field);
    Comparable o2 = (Comparable)t2.get(field);
    return o1.compareTo(o2);
  }
}