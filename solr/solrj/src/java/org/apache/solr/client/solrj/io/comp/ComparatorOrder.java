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
package org.apache.solr.client.solrj.io.comp;

import java.util.Locale;

/**
 * Enum for supported comparator ordering
 */
public enum ComparatorOrder {
  ASCENDING, DESCENDING;
  
  public static ComparatorOrder fromString(String order){
    switch(order.toLowerCase(Locale.ROOT)){
      case "asc":
        return ComparatorOrder.ASCENDING;
      case "desc":
        return ComparatorOrder.DESCENDING;
      default:
        throw new IllegalArgumentException(String.format(Locale.ROOT,"Unknown order '%s'", order));
    }
  }
  
  public String toString(){
    switch(this){
      case DESCENDING:
        return "desc";
      default:
        return "asc";
        
    }
  }
}
