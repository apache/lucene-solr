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
package org.apache.solr.common.params;

/**
 * Parameters and constants used when dealing with cursor based requests across 
 * large sorted result sets.
 */
public interface CursorMarkParams {

  /**
   * Param clients should specify indicating that they want a cursor based search.
   * The value specified must either be {@link #CURSOR_MARK_START} indicating the 
   * first page of results, or a value returned by a previous search via the 
   * {@link #CURSOR_MARK_NEXT} key.
   */
  public static final String CURSOR_MARK_PARAM = "cursorMark";

  /**
   * Key used in Solr response to inform the client what the "next" 
   * {@link #CURSOR_MARK_PARAM} value should be to continue pagination
   */
  public static final String CURSOR_MARK_NEXT = "nextCursorMark";

  /** 
   * Special value for {@link #CURSOR_MARK_PARAM} indicating that cursor functionality 
   * should be used, and a new cursor value should be computed afte the last result,
   * but that currently the "first page" of results is being requested
   */
  public static final String CURSOR_MARK_START = "*";

}

