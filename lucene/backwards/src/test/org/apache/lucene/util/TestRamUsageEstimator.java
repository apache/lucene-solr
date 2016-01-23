package org.apache.lucene.util;

/**
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

import junit.framework.TestCase;

public class TestRamUsageEstimator extends TestCase {

  public void testBasic() {
    String string = new String("test str");
    RamUsageEstimator rue = new RamUsageEstimator();
    long size = rue.estimateRamUsage(string);
    System.out.println("size:" + size);
    
    string = new String("test strin");
    size = rue.estimateRamUsage(string);
    System.out.println("size:" + size);
    
    Holder holder = new Holder();
    holder.holder = new Holder("string2", 5000L);
    size = rue.estimateRamUsage(holder);
    System.out.println("size:" + size);
    
    String[] strings = new String[]{new String("test strin"), new String("hollow"), new String("catchmaster")};
    size = rue.estimateRamUsage(strings);
    System.out.println("size:" + size);
  }
  
  private static final class Holder {
    long field1 = 5000L;
    String name = "name";
    Holder holder;
    
    Holder() {
    }
    
    Holder(String name, long field1) {
      this.name = name;
      this.field1 = field1;
    }
  }
}
