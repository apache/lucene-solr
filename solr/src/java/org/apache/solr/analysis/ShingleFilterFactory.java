
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
import org.apache.lucene.analysis.shingle.*;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Iterator;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import java.util.Map;
public class ShingleFilterFactory extends BaseTokenFilterFactory {
  private int maxShingleSize;
  private boolean outputUnigrams;
  public void init(Map<String, String> args) {
    super.init(args);
    maxShingleSize = getInt("maxShingleSize", 
                            ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE);
    outputUnigrams = getBoolean("outputUnigrams", true);
  }
  public ShingleFilter create(TokenStream input) {
    ShingleFilter r = new ShingleFilter(input,maxShingleSize);
    r.setOutputUnigrams(outputUnigrams);
    return r;
  }
}

