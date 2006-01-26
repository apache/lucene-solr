/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.LengthFilter;

import java.util.Map;

/**
 * @author yonik
 * @version $Id$
 */
public class LengthFilterFactory extends BaseTokenFilterFactory {
  int min,max;
  public void init(Map<String, String> args) {
    super.init(args);
    min=Integer.parseInt(args.get("min"));
    max=Integer.parseInt(args.get("max"));
  }
  public TokenStream create(TokenStream input) {
    return new LengthFilter(input,min,max);
  }
}
