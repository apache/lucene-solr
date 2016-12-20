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

/**
 * MBean interface for getting various ui friendly strings and URLs
 * for use by objects which are 'pluggable' to make server administration
 * easier.
 *
 *
 */
public interface SolrInfoMBean {

  /**
   * Category of {@link SolrCore} component.
   */
  enum Category { CORE, QUERYHANDLER, UPDATEHANDLER, CACHE, HIGHLIGHTING, QUERYPARSER, OTHER }

  /**
   * Top-level group of beans for a subsystem.
   */
  enum Group { jvm, jetty, http, node, core }

  /**
   * Simple common usage name, e.g. BasicQueryHandler,
   * or fully qualified clas name.
   */
  public String getName();
  /** Simple common usage version, e.g. 2.0 */
  public String getVersion();
  /** Simple one or two line description */
  public String getDescription();
  /** Purpose of this Class */
  public Category getCategory();
  /** CVS Source, SVN Source, etc */
  public String getSource();
  /**
   * Documentation URL list.
   *
   * <p>
   * Suggested documentation URLs: Homepage for sponsoring project,
   * FAQ on class usage, Design doc for class, Wiki, bug reporting URL, etc...
   * </p>
   */
  public URL[] getDocs();
  /**
   * Any statistics this instance would like to be publicly available via
   * the Solr Administration interface.
   *
   * <p>
   * Any Object type may be stored in the list, but only the
   * <code>toString()</code> representation will be used.
   * </p>
   */
  public NamedList getStatistics();

}
