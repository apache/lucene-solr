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
package org.apache.solr.core;

import java.net.URL;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoMBean.Category;

class MockInfoMBean implements SolrInfoMBean {
  @Override
  public String getName() {
    return "mock";
  }

  @Override
  public Category getCategory() {
    return Category.OTHER;
  }

  @Override
  public String getDescription() {
    return "mock";
  }

  @Override
  public URL[] getDocs() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getVersion() {
    return "mock";
  }

  @Override
  public String getSource() {
    return "mock";
  }

  @Override
  @SuppressWarnings("unchecked")
  public NamedList getStatistics() {
    NamedList myList = new NamedList<Integer>();
    myList.add("Integer", 123);
    myList.add("Double",567.534);
    myList.add("Long", 32352463l);
    myList.add("Short", (short) 32768);
    myList.add("Byte", (byte) 254);
    myList.add("Float", 3.456f);
    myList.add("String","testing");
    myList.add("Object", new Object());
    return myList;
  }
}