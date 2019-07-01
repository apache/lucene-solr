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
package org.apache.solr.rest.schema.analysis;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.rest.ManagedResource;

/**
 * TokenFilterFactory that uses the ManagedWordSetResource implementation
 * for managing stop words using the REST API.
 * @since 4.8.0
 * @lucene.spi {@value #NAME}
 */
public class ManagedStopFilterFactory extends BaseManagedTokenFilterFactory {

  /** SPI name */
  public static final String NAME = "managedStop";

  // this only gets changed once during core initialization and not every
  // time an update is made to the underlying managed word set.
  private CharArraySet stopWords = null;

  /**
   * Initialize the managed "handle"
   */
  public ManagedStopFilterFactory(Map<String,String> args) {
    super(args);    
  }
  
  /**
   * This analysis component knows the most logical "path"
   * for which to manage stop words from.
   */
  @Override
  public String getResourceId() {
    return "/schema/analysis/stopwords/" + handle;
  }
  
  /**
   * Returns the implementation class for managing stop words.
   */
  protected Class<? extends ManagedResource> getManagedResourceImplClass() {
    return ManagedWordSetResource.class;
  }
  
  /**
   * Callback invoked by the {@link ManagedResource} instance to trigger this
   * class to create the CharArraySet used to create the StopFilter using the
   * wordset managed by {@link ManagedWordSetResource}. Keep in mind that
   * a schema.xml may reuse the same {@link ManagedStopFilterFactory} many
   * times for different field types; behind the scenes all instances of this
   * class/handle combination share the same managed data, hence the need for
   * a listener/callback scheme.
   */
  @Override
  public void onManagedResourceInitialized(NamedList<?> args, ManagedResource res) 
      throws SolrException {

    Set<String> managedWords = ((ManagedWordSetResource)res).getWordSet(); 
        
    // first thing is to rebuild the Lucene CharArraySet from our managedWords set
    // which is slightly inefficient to do for every instance of the managed filter
    // but ManagedResource's don't have access to the luceneMatchVersion
    boolean ignoreCase = args.getBooleanArg("ignoreCase");
    stopWords = new CharArraySet(managedWords.size(), ignoreCase);
    stopWords.addAll(managedWords);
  }
       
  /**
   * Returns a StopFilter based on our managed stop word set.
   */
  @Override
  public TokenStream create(TokenStream input) {    
    if (stopWords == null) {
      throw new IllegalStateException("Managed stopwords not initialized correctly!");
    }
    return new StopFilter(input, stopWords);
  }
}
