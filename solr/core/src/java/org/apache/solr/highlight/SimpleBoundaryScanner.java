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
package org.apache.solr.highlight;

import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;

public class SimpleBoundaryScanner extends SolrBoundaryScanner {

  @Override
  protected BoundaryScanner get(String fieldName, SolrParams params) {
    int maxScan = params.getFieldInt(fieldName, HighlightParams.BS_MAX_SCAN, 10);
    String str = params.getFieldParam(fieldName, HighlightParams.BS_CHARS, ".,!? \t\n");
    Character[] chars = new Character[str.length()];
    for(int i = 0; i < str.length(); i++){
      chars[i] = str.charAt(i);
    }
    return new org.apache.lucene.search.vectorhighlight.SimpleBoundaryScanner(maxScan, chars);
  }


  ///////////////////////////////////////////////////////////////////////
  //////////////////////// SolrInfoMBeans methods ///////////////////////
  ///////////////////////////////////////////////////////////////////////
  
  @Override
  public String getDescription() {
    return "SimpleBoundaryScanner";
  }
}
