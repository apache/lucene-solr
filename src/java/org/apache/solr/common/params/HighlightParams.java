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

package org.apache.solr.common.params;

/**
 * 
 * @author ryan
 * @version $Id$
 * @since solr 1.3
 */
public interface HighlightParams {
  
  public static final String SIMPLE = "simple";
  
  public static final String HIGHLIGHT = "hl";
  public static final String PREFIX = "hl.";
  public static final String FIELDS = PREFIX+"fl";
  public static final String SNIPPETS = PREFIX+"snippets";
  public static final String FRAGSIZE = PREFIX+"fragsize";
  public static final String FORMATTER = PREFIX+"formatter";
  public static final String SIMPLE_PRE = PREFIX+SIMPLE+".pre";
  public static final String SIMPLE_POST = PREFIX+SIMPLE+".post";
  public static final String FIELD_MATCH = PREFIX+"requireFieldMatch";

}
