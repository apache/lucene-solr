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

package org.apache.solr.handler;

import org.apache.solr.handler.component.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 *
 *
 * All of the following options may be configured for this handler
 * in the solrconfig as defaults, and may be overriden as request parameters.
 * (TODO: complete documentation of request parameters here, rather than only
 * on the wiki).
 * </p>
 *
 * <ul>
 * <li> highlight - Set to any value not .equal() to "false" to enable highlight
 * generation</li>
 * <li> highlightFields - Set to a comma- or space-delimited list of fields to
 * highlight.  If unspecified, uses the default query field</li>
 * <li> maxSnippets - maximum number of snippets to generate per field-highlight.
 * </li>
 * </ul>
 *
 */
public class RealTimeGetHandler extends SearchHandler {
  @Override
  protected List<String> getDefaultComponents()
  {
    List<String> names = new ArrayList<String>(1);
    names.add(RealTimeGetComponent.COMPONENT_NAME);
    return names;
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getDescription() {
    return "The realtime get handler";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

  @Override
  public URL[] getDocs() {
    return null;
  }
}







