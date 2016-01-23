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

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.TrimFilter;
import org.apache.solr.common.SolrException;

/**
 * Factory for {@link TrimFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_trm" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.NGramTokenizerFactory"/&gt;
 *     &lt;filter class="solr.TrimFilterFactory" updateOffsets="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @see TrimFilter
 */
public class TrimFilterFactory extends BaseTokenFilterFactory {
  
  protected boolean updateOffsets = false;
  
  @Override
  public void init(Map<String,String> args) {
    super.init( args );
    
    String v = args.get( "updateOffsets" );
    if( v != null ) {
      try {
        updateOffsets = Boolean.valueOf( v );
      }
      catch( Exception ex ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Error reading updateOffsets value.  Must be true or false.", ex );
      }
    }
  }
  
  public TrimFilter create(TokenStream input) {
    return new TrimFilter(input, updateOffsets);
  }
}
