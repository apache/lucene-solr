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
 * Wraps a {@link SolrInfoMBean}.
 */
public class SolrInfoMBeanWrapper implements SolrInfoMBean {
  private final SolrInfoMBean mbean;

  public SolrInfoMBeanWrapper(SolrInfoMBean mbean) {
    this.mbean = mbean;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() { return mbean.getName(); }

  /** {@inheritDoc} */
  @Override
  public String getVersion() { return mbean.getVersion(); }

  /** {@inheritDoc} */
  @Override
  public String getDescription() { return mbean.getDescription(); }

  /** {@inheritDoc} */
  @Override
  public Category getCategory() { return mbean.getCategory(); }

  /** {@inheritDoc} */
  @Override
  public String getSource() { return mbean.getSource(); }

  /** {@inheritDoc} */
  @Override
  public URL[] getDocs() { return mbean.getDocs(); }

  /** {@inheritDoc} */
  @Override
  public NamedList getStatistics() { return mbean.getStatistics(); }

}
