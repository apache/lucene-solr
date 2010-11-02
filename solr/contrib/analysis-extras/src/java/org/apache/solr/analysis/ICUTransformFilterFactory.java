package org.apache.solr.analysis;

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

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.icu.ICUTransformFilter;
import org.apache.solr.analysis.BaseTokenFilterFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

import com.ibm.icu.text.Transliterator;

/**
 * Factory for {@link ICUTransformFilter}.
 * <p>
 * Supports the following attributes:
 * <ul>
 *   <li>id (mandatory): A Transliterator ID, one from {@link Transliterator#getAvailableIDs()}
 *   <li>direction (optional): Either 'forward' or 'reverse'. Default is forward.
 * </ul>
 * @see Transliterator
 */
public class ICUTransformFilterFactory extends BaseTokenFilterFactory {
  private Transliterator transliterator;
  
  // TODO: add support for custom rules
  @Override
  public void init(Map<String,String> args) {
    super.init(args);
    String id = args.get("id");
    if (id == null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "id is required.");
    }
    
    int dir;
    String direction = args.get("direction");
    if (direction == null || direction.equalsIgnoreCase("forward"))
      dir = Transliterator.FORWARD;
    else if (direction.equalsIgnoreCase("reverse"))
      dir = Transliterator.REVERSE;
    else
      throw new SolrException(ErrorCode.SERVER_ERROR, "invalid direction: " + direction);
    
    transliterator = Transliterator.getInstance(id, dir);
  }

  public TokenStream create(TokenStream input) {
    return new ICUTransformFilter(input, transliterator);
  }
}
