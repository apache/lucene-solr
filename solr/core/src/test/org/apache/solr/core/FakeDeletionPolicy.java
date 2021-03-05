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

import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class FakeDeletionPolicy extends IndexDeletionPolicy implements NamedListInitializedPlugin {

  private String var1;
  private String var2;

  //@Override
  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    var1 = (String) args.get("var1");
    var2 = (String) args.get("var2");
  }

  public String getVar1() {
    return var1;
  }

  public String getVar2() {
    return var2;
  }

  //  @Override
  @Override
  public void onCommit(@SuppressWarnings({"rawtypes"})List arg0) throws IOException {
    System.setProperty("onCommit", "test.org.apache.solr.core.FakeDeletionPolicy.onCommit");
  }

  //  @Override
  @Override
  public void onInit(@SuppressWarnings({"rawtypes"})List arg0) throws IOException {
    System.setProperty("onInit", "test.org.apache.solr.core.FakeDeletionPolicy.onInit");
  }
}
