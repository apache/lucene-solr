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
import org.apache.lucene.search.vectorhighlight.FragmentsBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

public abstract class SolrFragmentsBuilder extends HighlightingPluginBase
  implements SolrInfoBean, NamedListInitializedPlugin {
  
  public static final String DEFAULT_PRE_TAGS = "<em>";
  public static final String DEFAULT_POST_TAGS = "</em>";

  /**
   * Return a {@link org.apache.lucene.search.vectorhighlight.FragmentsBuilder} appropriate for this field.
   * 
   * @param params The params controlling Highlighting
   * @return An appropriate {@link org.apache.lucene.search.vectorhighlight.FragmentsBuilder}.
   */
  public FragmentsBuilder getFragmentsBuilder(SolrParams params, BoundaryScanner bs) {
    numRequests.inc();
    params = SolrParams.wrapDefaults(params, defaults);

    return getFragmentsBuilder( params, getPreTags( params, null ), getPostTags( params, null ), bs );
  }
  
  public String[] getPreTags( SolrParams params, String fieldName ){
    return getTags( params, HighlightParams.TAG_PRE, fieldName, DEFAULT_PRE_TAGS );
  }
  
  public String[] getPostTags( SolrParams params, String fieldName ){
    return getTags( params, HighlightParams.TAG_POST, fieldName, DEFAULT_POST_TAGS );
  }
  
  private String[] getTags( SolrParams params, String paramName, String fieldName, String def ){
    params = SolrParams.wrapDefaults(params, defaults);

    String value = null;
    if( fieldName == null )
      value = params.get( paramName, def );
    else
      value = params.getFieldParam( fieldName, paramName, def );
    String[] tags = value.split( "," );
    for( int i = 0; i < tags.length; i++ ){
      tags[i] = tags[i].trim();
    }
    return tags;
  }
  
  protected abstract FragmentsBuilder getFragmentsBuilder( SolrParams params,
      String[] preTags, String[] postTags, BoundaryScanner bs );
  
  protected char getMultiValuedSeparatorChar( SolrParams params ){
    String separator = params.get( HighlightParams.MULTI_VALUED_SEPARATOR, " " );
    if( separator.length() > 1 ){
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          HighlightParams.MULTI_VALUED_SEPARATOR + " parameter must be a char, but is \"" + separator + "\"" );
    }
    return separator.charAt( 0 );
  }
}
