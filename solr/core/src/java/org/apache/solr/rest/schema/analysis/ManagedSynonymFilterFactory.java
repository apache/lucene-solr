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
import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.FlattenGraphFilterFactory;  // javadocs
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymFilterFactory;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
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
 * TokenFilterFactory and ManagedResource implementation for 
 * doing CRUD on synonyms using the REST API.
 * 
 * @deprecated Use {@link ManagedSynonymGraphFilterFactory} instead, but be sure to also
 * use {@link FlattenGraphFilterFactory} at index time (not at search time) as well.
 * @since 4.8.0
 * @lucene.spi {@value #NAME}
 */
@Deprecated
public class ManagedSynonymFilterFactory extends BaseManagedTokenFilterFactory {

  /** SPI name */
  public static final String NAME = "managedSynonym";
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final String SYNONYM_MAPPINGS = "synonymMappings";
  public static final String IGNORE_CASE_INIT_ARG = "ignoreCase";

  /**
   * Used internally to preserve the case of synonym mappings regardless
   * of the ignoreCase setting.
   */
  private static class CasePreservedSynonymMappings {
    Map<String,Set<String>> mappings = new TreeMap<>();
    
    /**
     * Provides a view of the mappings for a given term; specifically, if
     * ignoreCase is true, then the returned "view" contains the mappings
     * for all known cases of the term, if it is false, then only the
     * mappings for the specific case is returned. 
     */
    Set<String> getMappings(boolean ignoreCase, String key) {
      Set<String> synMappings = null;
      if (ignoreCase) {
        // TODO: should we return the mapped values in all lower-case here?
        if (mappings.size() == 1) {
          // if only one in the map (which is common) just return it directly
          return mappings.values().iterator().next();
        }
        
        synMappings = new TreeSet<>();
        for (Set<String> next : mappings.values())
          synMappings.addAll(next);
      } else {
        synMappings = mappings.get(key);
      }
      return synMappings;
    }
    
    public String toString() {
      return mappings.toString();
    }
  }
  
  /**
   * ManagedResource implementation for synonyms, which are so specialized that
   * it makes sense to implement this class as an inner class as it has little 
   * application outside the SynonymFilterFactory use cases.
   */
  public static class SynonymManager extends ManagedResource 
      implements ManagedResource.ChildResourceSupport
  {
    protected Map<String,CasePreservedSynonymMappings> synonymMappings;

    public SynonymManager(String resourceId, SolrResourceLoader loader, StorageIO storageIO)
        throws SolrException {
      super(resourceId, loader, storageIO);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void onManagedDataLoadedFromStorage(NamedList<?> managedInitArgs, Object managedData)
        throws SolrException
    {
      NamedList<Object> initArgs = (NamedList<Object>)managedInitArgs;
      
      String format = (String)initArgs.get("format");
      if (format != null && !"solr".equals(format)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid format "+
           format+"! Only 'solr' is supported.");
      }
      
      // the default behavior is to not ignore case, 
      // so if not supplied, then install the default
      if (initArgs.get(IGNORE_CASE_INIT_ARG) == null) {
        initArgs.add(IGNORE_CASE_INIT_ARG, Boolean.FALSE);
      }

      boolean ignoreCase = getIgnoreCase(managedInitArgs);
      synonymMappings = new TreeMap<>();
      if (managedData != null) {
        Map<String,Object> storedSyns = (Map<String,Object>)managedData;
        for (Map.Entry<String, Object> entry : storedSyns.entrySet()) {
          String key = entry.getKey();

          String caseKey = applyCaseSetting(ignoreCase, key);
          CasePreservedSynonymMappings cpsm = synonymMappings.get(caseKey);
          if (cpsm == null) {
            cpsm = new CasePreservedSynonymMappings();
            synonymMappings.put(caseKey, cpsm);
          }
          
          // give the nature of our JSON parsing solution, we really have
          // no guarantees on what is in the file
          Object mapping = entry.getValue();
          if (!(mapping instanceof List)) {
            throw new SolrException(ErrorCode.SERVER_ERROR, 
                "Invalid synonym file format! Expected a list of synonyms for "+key+
                " but got "+mapping.getClass().getName());
          }

          Set<String> sortedVals = new TreeSet<>((List<String>) entry.getValue());
          cpsm.mappings.put(key, sortedVals);        
        }
      }
      if (log.isInfoEnabled()) {
        log.info("Loaded {} synonym mappings for {}", synonymMappings.size(), getResourceId());
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object applyUpdatesToManagedData(Object updates) {
      boolean ignoreCase = getIgnoreCase();
      boolean madeChanges = false;
      if (updates instanceof List) {
        madeChanges = applyListUpdates((List<String>)updates, ignoreCase);
      } else if (updates instanceof Map) {
        madeChanges = applyMapUpdates((Map<String,Object>)updates, ignoreCase);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Unsupported data format (" + updates.getClass().getName() + "); expected a JSON object (Map or List)!");
      }
      return madeChanges ? getStoredView() : null;
    }

    protected boolean applyListUpdates(List<String> jsonList, boolean ignoreCase) {
      boolean madeChanges = false;
      for (String term : jsonList) {
        // find the mappings using the case aware key
        String origTerm = term;
        term = applyCaseSetting(ignoreCase, term);
        CasePreservedSynonymMappings cpsm = synonymMappings.get(term);
        if (cpsm == null)
          cpsm = new CasePreservedSynonymMappings();

        Set<String> treeTerms = new TreeSet<>(jsonList);
        cpsm.mappings.put(origTerm, treeTerms);
        madeChanges = true;
        // only add the cpsm to the synonymMappings if it has valid data
        if (!synonymMappings.containsKey(term) && cpsm.mappings.get(origTerm) != null) {
          synonymMappings.put(term, cpsm);
        }
      }
      return madeChanges;
    }

    protected boolean applyMapUpdates(Map<String,Object> jsonMap, boolean ignoreCase) {
      boolean madeChanges = false;

      for (String term : jsonMap.keySet()) {

        String origTerm = term;
        term = applyCaseSetting(ignoreCase, term);

        // find the mappings using the case aware key
        CasePreservedSynonymMappings cpsm = synonymMappings.get(term);
        if (cpsm == null)
          cpsm = new CasePreservedSynonymMappings();

        Set<String> output = cpsm.mappings.get(origTerm);

        Object val = jsonMap.get(origTerm); // IMPORTANT: use the original
        if (val instanceof String) {
          String strVal = (String)val;

          if (output == null) {
            output = new TreeSet<>();
            cpsm.mappings.put(origTerm, output);
          }

          if (output.add(strVal)) {
            madeChanges = true;
          }
        } else if (val instanceof List) {
          @SuppressWarnings({"unchecked"})
          List<String> vals = (List<String>)val;

          if (output == null) {
            output = new TreeSet<>();
            cpsm.mappings.put(origTerm, output);
          }

          for (String nextVal : vals) {
            if (output.add(nextVal)) {
              madeChanges = true;
            }
          }

        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Unsupported value "+val+
              " for "+term+"; expected single value or a JSON array!");
        }

        // only add the cpsm to the synonymMappings if it has valid data
        if (!synonymMappings.containsKey(term) && cpsm.mappings.get(origTerm) != null) {
          synonymMappings.put(term, cpsm);
        }
      }

      return madeChanges;
    }
    
    /**
     * Returns a Map of how we store and load data managed by this resource,
     * which is different than how it is managed at runtime in order to support
     * the ignoreCase setting. 
     */
    protected Map<String,Set<String>> getStoredView() {
      Map<String,Set<String>> storedView = new TreeMap<>();
      for (CasePreservedSynonymMappings cpsm : synonymMappings.values()) {
        for (Map.Entry<String, Set<String>> entry : cpsm.mappings.entrySet()) {
          storedView.put(entry.getKey(), entry.getValue());
        }
      }
      return storedView;
    }
        
    protected String applyCaseSetting(boolean ignoreCase, String str) {
      return (ignoreCase && str != null) ? str.toLowerCase(Locale.ROOT) : str;
    }
    
    public boolean getIgnoreCase() {
      return getIgnoreCase(managedInitArgs);
    }

    public boolean getIgnoreCase(NamedList<?> initArgs) {
      Boolean ignoreCase = initArgs.getBooleanArg(IGNORE_CASE_INIT_ARG);
      // ignoreCase = false by default
      return null == ignoreCase ? false : ignoreCase;
    }
    
    @Override
    public void doGet(BaseSolrResource endpoint, String childId) {
      SolrQueryResponse response = endpoint.getSolrResponse();
      if (childId != null) {
        boolean ignoreCase = getIgnoreCase();
        String key = applyCaseSetting(ignoreCase, childId);
        
        // if ignoreCase==true, then we get the mappings using the lower-cased key
        // and then return a union of all case-sensitive keys, if false, then
        // we only return the mappings for the exact case requested
        CasePreservedSynonymMappings cpsm = synonymMappings.get(key);
        Set<String> mappings = (cpsm != null) ? cpsm.getMappings(ignoreCase, childId) : null;
        if (mappings == null)
          throw new SolrException(ErrorCode.NOT_FOUND,
              String.format(Locale.ROOT, "%s not found in %s", childId, getResourceId()));          
        
        response.add(childId, mappings);
      } else {
        response.add(SYNONYM_MAPPINGS, buildMapToStore(getStoredView()));      
      }
    }  

    @Override
    public synchronized void doDeleteChild(BaseSolrResource endpoint, String childId) {
      boolean ignoreCase = getIgnoreCase();
      String key = applyCaseSetting(ignoreCase, childId);
      
      CasePreservedSynonymMappings cpsm = synonymMappings.get(key);
      if (cpsm == null)
        throw new SolrException(ErrorCode.NOT_FOUND, 
            String.format(Locale.ROOT, "%s not found in %s", childId, getResourceId()));

      if (ignoreCase) {
        // delete all mappings regardless of case
        synonymMappings.remove(key);
      } else {
        // just delete the mappings for the specific case-sensitive key
        if (cpsm.mappings.containsKey(childId)) {
          cpsm.mappings.remove(childId);
          
          if (cpsm.mappings.isEmpty())
            synonymMappings.remove(key);            
        } else {
          throw new SolrException(ErrorCode.NOT_FOUND, 
              String.format(Locale.ROOT, "%s not found in %s", childId, getResourceId()));          
        }
      }
      
      // store the updated data (using the stored view)
      storeManagedData(getStoredView());
      
      log.info("Removed synonym mappings for: {}", childId);      
    }
  }
  
  /**
   * Custom SynonymMap.Parser implementation that provides synonym
   * mappings from the managed JSON in this class during SynonymMap
   * building.
   */
  private class ManagedSynonymParser extends SynonymMap.Parser {

    SynonymManager synonymManager;
    
    public ManagedSynonymParser(SynonymManager synonymManager, boolean dedup, Analyzer analyzer) {
      super(dedup, analyzer);
      this.synonymManager = synonymManager;
    }

    /**
     * Add the managed synonyms and their mappings into the SynonymMap builder.
     */
    @Override
    public void parse(Reader in) throws IOException, ParseException {
      boolean ignoreCase = synonymManager.getIgnoreCase();
      for (CasePreservedSynonymMappings cpsm : synonymManager.synonymMappings.values()) {
        for (Map.Entry<String, Set<String>> entry : cpsm.mappings.entrySet()) {
          for (String mapping : entry.getValue()) {
            // apply the case setting to match the behavior of the SynonymMap builder
            CharsRef casedTerm = analyze(synonymManager.applyCaseSetting(ignoreCase, entry.getKey()), new CharsRefBuilder());
            CharsRef casedMapping = analyze(synonymManager.applyCaseSetting(ignoreCase, mapping), new CharsRefBuilder());
            add(casedTerm, casedMapping, false);
          }          
        }
      }      
    }    
  }
  
  protected SynonymFilterFactory delegate;
          
  public ManagedSynonymFilterFactory(Map<String,String> args) {
    super(args);    
  }

  @Override
  public String getResourceId() {
    return "/schema/analysis/synonyms/"+handle;
  }  
    
  protected Class<? extends ManagedResource> getManagedResourceImplClass() {
    return SynonymManager.class;
  }

  /**
   * Called once, during core initialization, to initialize any analysis components
   * that depend on the data managed by this resource. It is important that the
   * analysis component is only initialized once during core initialization so that
   * text analysis is consistent, especially in a distributed environment, as we
   * don't want one server applying a different set of stop words than other servers.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void onManagedResourceInitialized(NamedList<?> initArgs, final ManagedResource res) 
      throws SolrException
  {    
    NamedList<Object> args = (NamedList<Object>)initArgs;    
    args.add("synonyms", getResourceId());
    args.add("expand", "false");
    args.add("format", "solr");
    
    Map<String,String> filtArgs = new HashMap<>();
    for (Map.Entry<String,?> entry : args) {
      filtArgs.put(entry.getKey(), entry.getValue().toString());
    }
    // create the actual filter factory that pulls the synonym mappings
    // from synonymMappings using a custom parser implementation
    delegate = new SynonymFilterFactory(filtArgs) {
      @Override
      protected SynonymMap loadSynonyms
          (ResourceLoader loader, String cname, boolean dedup, Analyzer analyzer)
          throws IOException, ParseException {

        ManagedSynonymParser parser =
            new ManagedSynonymParser((SynonymManager)res, dedup, analyzer);
        // null is safe here because there's no actual parsing done against a input Reader
        parser.parse(null);
        return parser.build(); 
      }
    };
    try {
      delegate.inform(res.getResourceLoader());
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }    
  }
    
  @Override
  public TokenStream create(TokenStream input) {    
    if (delegate == null)
      throw new IllegalStateException(this.getClass().getName()+
          " not initialized correctly! The SynonymFilterFactory delegate was not initialized.");
    
    return delegate.create(input);
  }
}
