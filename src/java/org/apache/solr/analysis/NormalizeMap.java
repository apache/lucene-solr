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

package org.apache.solr.analysis;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @version $Id$
 * @since Solr 1.4
 *
 */
public class NormalizeMap {

  Map<Character, NormalizeMap> submap;
  String normStr;
  int diff;

  public void add( String singleMatch, String replacement ){
    NormalizeMap currMap = this;
    for( int i = 0; i < singleMatch.length(); i++ ){
      char c = singleMatch.charAt( i );
      if( currMap.submap == null ){
        currMap.submap = new HashMap<Character, NormalizeMap>( 1 );
      }
      NormalizeMap map = currMap.submap.get( c );
      if( map == null ){
        map = new NormalizeMap();
        currMap.submap.put( c, map );
      }
      currMap = map;
    }
    if( currMap.normStr != null ){
      throw new RuntimeException( "MappingCharFilter: there is already a mapping for " + singleMatch );
    }
    currMap.normStr = replacement;
    currMap.diff = singleMatch.length() - replacement.length();
  }
}
