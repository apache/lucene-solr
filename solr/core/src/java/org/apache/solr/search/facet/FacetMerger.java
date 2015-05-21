package org.apache.solr.search.facet;

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

//
// The FacetMerger code is in the prototype stage, and this is the reason that
// many implementations are all in this file.  They can be moved to separate
// files after the interfaces are locked down more.
//
public abstract class FacetMerger {
  public abstract void merge(Object facetResult, Context mcontext);
  public abstract Object getMergedResult();

  public static class Context {
    // FacetComponentState state;  // todo: is this needed?
    Object root;
  }
}
