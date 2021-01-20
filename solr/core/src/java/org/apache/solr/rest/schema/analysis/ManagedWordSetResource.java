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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ManagedResource implementation for managing a set of words using the REST API;
 * useful for managing stop words and/or protected words for analysis components 
 * like the KeywordMarkerFilter.
 */
public class ManagedWordSetResource extends ManagedResource 
  implements ManagedResource.ChildResourceSupport {
  
  public static final String WORD_SET_JSON_FIELD = "wordSet";
  public static final String IGNORE_CASE_INIT_ARG = "ignoreCase";
      
  private SortedSet<String> managedWords = null;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public ManagedWordSetResource(String resourceId, SolrResourceLoader loader, StorageIO storageIO) 
      throws SolrException {
    super(resourceId, loader, storageIO);
  }

  /**
   * Returns the set of words in this managed word set.
   */
  public Set<String> getWordSet() {
    return Collections.unmodifiableSet(managedWords);
  }

  /**
   * Returns the boolean value of the {@link #IGNORE_CASE_INIT_ARG} init arg,
   * or the default value (false) if it has not been specified
   */
  public boolean getIgnoreCase() {
    return getIgnoreCase(managedInitArgs);
  }

  /**
   * Returns the boolean value of the {@link #IGNORE_CASE_INIT_ARG} init arg,
   * or the default value (false) if it has not been specified
   */
  public boolean getIgnoreCase(NamedList<?> initArgs) {
    Boolean ignoreCase = initArgs.getBooleanArg(IGNORE_CASE_INIT_ARG);
    // ignoreCase = false by default
    return null == ignoreCase ? false : ignoreCase;
  }
               
  /**
   * Invoked when loading data from storage to initialize the 
   * list of words managed by this instance. A load of the
   * data can happen many times throughout the life cycle of this
   * object.
   */
  @SuppressWarnings("unchecked")
  @Override
  protected void onManagedDataLoadedFromStorage(NamedList<?> initArgs, Object data)
      throws SolrException {

    // the default behavior is to not ignore case,
    boolean ignoreCase = getIgnoreCase(initArgs);
    if (null == initArgs.get(IGNORE_CASE_INIT_ARG)) {
      // Explicitly include the default value of ignoreCase
      ((NamedList<Object>)initArgs).add(IGNORE_CASE_INIT_ARG, false);
    }

    managedWords = new TreeSet<>();
    if (data != null) {
      List<String> wordList = (List<String>)data;
      if (ignoreCase) {
        // if we're ignoring case, just lowercase all terms as we add them
        for (String word : wordList) {
          managedWords.add(word.toLowerCase(Locale.ROOT));
        }
      } else {
        managedWords.addAll(wordList);        
      }
    } else {
      storeManagedData(new ArrayList<String>(0)); // stores an empty word set      
    }
    if (log.isInfoEnabled()) {
      log.info("Loaded {} words for {}", managedWords.size(), getResourceId());
    }
  }
          
  /**
   * Implements the GET request to provide the list of words to the client.
   * Alternatively, if a specific word is requested, then it is returned
   * or a 404 is raised, indicating that the requested word does not exist.
   */
  @Override
  public void doGet(BaseSolrResource endpoint, String childId) {
    SolrQueryResponse response = endpoint.getSolrResponse();
    if (childId != null) {
      // downcase arg if we're configured to ignoreCase
      String key = getIgnoreCase() ? childId.toLowerCase(Locale.ROOT) : childId;       
      if (!managedWords.contains(key))
        throw new SolrException(ErrorCode.NOT_FOUND, 
            String.format(Locale.ROOT, "%s not found in %s", childId, getResourceId()));
        
      response.add(childId, key);
    } else {
      response.add(WORD_SET_JSON_FIELD, buildMapToStore(managedWords));      
    }
  }  

  /**
   * Deletes words managed by this resource.
   */
  @Override
  public synchronized void doDeleteChild(BaseSolrResource endpoint, String childId) {
    // downcase arg if we're configured to ignoreCase
    String key = getIgnoreCase() ? childId.toLowerCase(Locale.ROOT) : childId;       
    if (!managedWords.contains(key))
      throw new SolrException(ErrorCode.NOT_FOUND, 
          String.format(Locale.ROOT, "%s not found in %s", childId, getResourceId()));
  
    managedWords.remove(key);
    storeManagedData(managedWords);
    log.info("Removed word: {}", key);
  }  
  
  /**
   * Applies updates to the word set being managed by this resource.
   */
  @SuppressWarnings("unchecked")
  @Override
  protected Object applyUpdatesToManagedData(Object updates) {
    boolean madeChanges = false;
    List<String> words = (List<String>)updates;
    
    log.info("Applying updates: {}", words);
    boolean ignoreCase = getIgnoreCase();    
    for (String word : words) {
      if (ignoreCase)
        word = word.toLowerCase(Locale.ROOT);
      
      if (managedWords.add(word)) {
        madeChanges = true;
        log.info("Added word: {}", word);
      }
    }              
    return madeChanges ? managedWords : null;
  }
  
  @Override
  protected boolean updateInitArgs(NamedList<?> updatedArgs) {
    if (updatedArgs == null || updatedArgs.size() == 0) {
      return false;
    }
    boolean currentIgnoreCase = getIgnoreCase(managedInitArgs);
    boolean updatedIgnoreCase = getIgnoreCase(updatedArgs);
    if (currentIgnoreCase == true && updatedIgnoreCase == false) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Changing a managed word set's ignoreCase arg from true to false is not permitted.");
    } else if (currentIgnoreCase == false && updatedIgnoreCase == true) {
      // rebuild the word set on policy change from case-sensitive to case-insensitive
      SortedSet<String> updatedWords = new TreeSet<>();
      for (String word : managedWords) {
        updatedWords.add(word.toLowerCase(Locale.ROOT));
      }
      managedWords = updatedWords;
    }
    // otherwise currentIgnoreCase == updatedIgnoreCase: nothing to do
    return super.updateInitArgs(updatedArgs);
  }  
}
