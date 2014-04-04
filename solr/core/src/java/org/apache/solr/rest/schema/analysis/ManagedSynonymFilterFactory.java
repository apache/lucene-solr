package org.apache.solr.rest.schema.analysis;
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

import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymFilterFactory;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.util.CharsRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.restlet.data.Status;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TokenFilterFactory and ManagedResource implementation for 
 * doing CRUD on synonyms using the REST API.
 */
public class ManagedSynonymFilterFactory extends BaseManagedTokenFilterFactory {
  
  public static final Logger log = LoggerFactory.getLogger(ManagedSynonymFilterFactory.class);
  
  public static final String SYNONYM_MAPPINGS = "synonymMappings";
  public static final String IGNORE_CASE_INIT_ARG = "ignoreCase";
  
  /**
   * ManagedResource implementation for synonyms, which are so specialized that
   * it makes sense to implement this class as an inner class as it has little 
   * application outside the SynonymFilterFactory use cases.
   */
  public static class SynonymManager extends ManagedResource 
      implements ManagedResource.ChildResourceSupport
  {

    // TODO: Maybe hold this using a SoftReference / WeakReference to
    // reduce memory in case the set of synonyms is large and the JVM 
    // is running low on memory?
    protected Map<String,Set<String>> synonymMappings;
    
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
        for (String key : storedSyns.keySet()) {
          // give the nature of our JSON parsing solution, we really have
          // no guarantees on what is in the file
          Object mapping = storedSyns.get(key);
          if (!(mapping instanceof List)) {
            throw new SolrException(ErrorCode.SERVER_ERROR, 
                "Invalid synonym file format! Expected a list of synonyms for "+key+
                " but got "+mapping.getClass().getName());
          }
                    
          // if we're configured to ignoreCase, then we build the mappings with all lower           
          List<String> vals = (List<String>)storedSyns.get(key);
          Set<String> sortedVals = new TreeSet<>();
          if (ignoreCase) {
            for (String next : vals) {
              sortedVals.add(applyCaseSetting(ignoreCase, next));
            }
          } else {
            sortedVals.addAll(vals);
          }
          
          synonymMappings.put(applyCaseSetting(ignoreCase, key), sortedVals);
        }
      }
      
      log.info("Loaded {} synonym mappings for {}", synonymMappings.size(), getResourceId());      
    }    

    @SuppressWarnings("unchecked")
    @Override
    protected Object applyUpdatesToManagedData(Object updates) {
      if (!(updates instanceof Map)) {
        throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
          "Unsupported data format (" + updates.getClass().getName() + "); expected a JSON object (Map)!");
      }
      boolean ignoreCase = getIgnoreCase();      
      boolean madeChanges = false;
      Map<String,Object> jsonMap = (Map<String,Object>)updates;
      for (String term : jsonMap.keySet()) {
        
        term = applyCaseSetting(ignoreCase, term);
        
        Set<String> output = synonymMappings.get(term); 
        
        Object val = jsonMap.get(term);
        if (val instanceof String) {
          String strVal = applyCaseSetting(ignoreCase, (String)val);
          
          if (output == null) {
            output = new TreeSet<>();
            synonymMappings.put(term, output);
          }
                    
          if (output.add(strVal)) {
            madeChanges = true;
          }
        } else if (val instanceof List) {
          List<String> vals = (List<String>)val;
          
          if (output == null) {
            output = new TreeSet<>();
            synonymMappings.put(term, output);
          }
          
          for (String nextVal : vals) {
            if (output.add(applyCaseSetting(ignoreCase, nextVal))) {
              madeChanges = true;
            }
          }          
          
        } else {
          throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, "Unsupported value "+val+
              " for "+term+"; expected single value or a JSON array!");
        }
      }
          
      return madeChanges ? synonymMappings : null;
    }
    
    /**
     * Handles a change in the ignoreCase setting for synonyms, which requires
     * a full rebuild of the synonymMappings.
     */
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
        // ignore case policy changed ... rebuild the map
        Map<String,Set<String>> rebuild = new TreeMap<>();
        for (String curr : synonymMappings.keySet()) {
          Set<String> newMappings = new TreeSet<>();
          for (String next : synonymMappings.get(curr)) {
            newMappings.add(applyCaseSetting(updatedIgnoreCase, next));
          }
          rebuild.put(applyCaseSetting(updatedIgnoreCase, curr), newMappings);
        }
        synonymMappings = rebuild;
      }
      
      return super.updateInitArgs(updatedArgs);
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
        Set<String> output = synonymMappings.get(key);
        if (output == null) {
          throw new SolrException(ErrorCode.NOT_FOUND,
              String.format(Locale.ROOT, "%s not found in %s", key, getResourceId()));
        }
        response.add(key, output);
      } else {
        response.add(SYNONYM_MAPPINGS, buildMapToStore(synonymMappings));      
      }
    }  

    @Override
    public synchronized void doDeleteChild(BaseSolrResource endpoint, String childId) {
      boolean ignoreCase = getIgnoreCase();
      String key = applyCaseSetting(ignoreCase, childId);
      Set<String> output = synonymMappings.get(key);
      if (output == null)
        throw new SolrException(ErrorCode.NOT_FOUND, 
            String.format(Locale.ROOT, "%s not found in %s", key, getResourceId()));
      
      synonymMappings.remove(key);
      storeManagedData(synonymMappings);
      log.info("Removed synonym mappings for: {}", key);      
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
      for (String term : synonymManager.synonymMappings.keySet()) {
        for (String mapping : synonymManager.synonymMappings.get(term)) {
          add(new CharsRef(term), new CharsRef(mapping), false);
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
