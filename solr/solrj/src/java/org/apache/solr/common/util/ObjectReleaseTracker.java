package org.apache.solr.common.util;

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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ObjectReleaseTracker {
  public static Map<Object,String> OBJECTS = new ConcurrentHashMap<>();
  
  public static boolean track(Object object) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    new ObjectTrackerException().printStackTrace(pw);
    OBJECTS.put(object, sw.toString());
    return true;
  }
  
  public static boolean release(Object object) {
    OBJECTS.remove(object);
    return true;
  }
  
  /**
   * @return null if ok else error message
   */
  public static String clearObjectTrackerAndCheckEmpty() {
    String error = null;
    Set<Entry<Object,String>> entries = OBJECTS.entrySet();
    boolean empty = entries.isEmpty();
    if (entries.size() > 0) {
      Set<String> objects = new HashSet<>();
      for (Entry<Object,String> entry : entries) {
        objects.add(entry.getKey().getClass().getSimpleName());
      }
      
      error = "ObjectTracker found " + entries.size() + " object(s) that were not released!!! " + objects;
      
      System.err.println(error);
      for (Entry<Object,String> entry : entries) {
        System.err.println(entry.getValue());
      }
    }
    
    OBJECTS.clear();
    
    return error;
  }
  
  private static class ObjectTrackerException extends RuntimeException {
    
  }
}
