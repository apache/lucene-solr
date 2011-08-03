package org.apache.lucene.queryParser.standard.config;

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

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.standard.nodes.RangeQueryNode;
import org.apache.lucene.queryParser.standard.processors.ParametricRangeQueryNodeProcessor;
import org.apache.lucene.util.Attribute;

/**
 * This attribute is used by {@link ParametricRangeQueryNodeProcessor} processor
 * and must be defined in the {@link QueryConfigHandler}. This attribute tells
 * the processor which {@link Resolution} to use when parsing the date. <br/>
 * 
 * @deprecated
 * 
 */
@Deprecated
public interface DateResolutionAttribute extends Attribute {
  /**
   * Sets the default date resolution used by {@link RangeQueryNode}s for
   * fields for which no specific date resolutions has been set. Field
   * specific resolutions can be set with
   * 
   * @param dateResolution the default date resolution to set
   */
  public void setDateResolution(DateTools.Resolution dateResolution);
  public DateTools.Resolution getDateResolution();
}
