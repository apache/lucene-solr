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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.update.processor.FieldMutatingUpdateProcessorFactory.SelectorParams;

/**
 * Reusable base class for UpdateProcessors that will consider 
 * AddUpdateCommands and mutate the values associated with configured
 * fields.
 * <p>
 * Subclasses should override the mutate method to specify how individual 
 * SolrInputFields identified by the selector associated with this instance 
 * will be mutated.
 * </p>
 *
 * @see FieldMutatingUpdateProcessorFactory
 * @see FieldValueMutatingUpdateProcessor
 * @see FieldNameSelector
 */
public abstract class FieldMutatingUpdateProcessor 
  extends UpdateRequestProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final FieldNameSelector selector;
  public FieldMutatingUpdateProcessor(FieldNameSelector selector,
                                      UpdateRequestProcessor next) {
    super(next);
    this.selector = selector;
  }
  
  /**
   * Method for mutating SolrInputFields associated with fields identified 
   * by the FieldNameSelector associated with this processor
   * @param src the SolrInputField to mutate, may be modified in place and 
   *            returned
   * @return the SolrInputField to use in replacing the original (src) value.
   *         If null the field will be removed.
   */
  protected abstract SolrInputField mutate(final SolrInputField src);
  
  /**
   * Calls <code>mutate</code> on any fields identified by the selector 
   * before forwarding the command down the chain.  Any SolrExceptions 
   * thrown by <code>mutate</code> will be logged with the Field name, 
   * wrapped and re-thrown.
   */
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    final SolrInputDocument doc = cmd.getSolrInputDocument();

    // make a copy we can iterate over while mutating the doc
    final Collection<String> fieldNames 
      = new ArrayList<>(doc.getFieldNames());

    for (final String fname : fieldNames) {

      if (! selector.shouldMutate(fname)) continue;
      
      final SolrInputField src = doc.get(fname);

      SolrInputField dest = null;
      try { 
        dest = mutate(src);
      } catch (SolrException e) {
        String msg = "Unable to mutate field '"+fname+"': "+e.getMessage();
        SolrException.log(log, msg, e);
        throw new SolrException(BAD_REQUEST, msg, e);
      }
      if (null == dest) {
        doc.remove(fname);
      } else {
        // semantics of what happens if dest has diff name are hard
        // we could treat it as a copy, or a rename
        // for now, don't allow it.
        if (! fname.equals(dest.getName()) ) {
          throw new SolrException(SERVER_ERROR,
                                  "mutate returned field with different name: " 
                                  + fname + " => " + dest.getName());
        }
        doc.put(dest.getName(), dest);
      }
    }
    super.processAdd(cmd);
  }
  
  /**
   * Interface for identifying which fields should be mutated
   */
  public interface FieldNameSelector {
    boolean shouldMutate(final String fieldName);
  }

  /** Singleton indicating all fields should be mutated */
  public static final FieldNameSelector SELECT_ALL_FIELDS = fieldName -> true;

  /** Singleton indicating no fields should be mutated */
  public static final FieldNameSelector SELECT_NO_FIELDS = fieldName -> false;

  /** 
   * Wraps two FieldNameSelectors such that the FieldNameSelector 
   * returned matches all fields specified by the "includes" unless they 
   * are matched by "excludes"
   * @param includes a selector identifying field names that should be selected
   * @param excludes a selector identifying field names that should be 
   *        <i>not</i> be selected, even if they are matched by the 'includes' 
   *        selector
   * @return Either a new FieldNameSelector or one of the input selecors 
   *         if the combination lends itself to optimization.
   */
  public static FieldNameSelector wrap(final FieldNameSelector includes, 
                                       final FieldNameSelector excludes) { 

    if (SELECT_NO_FIELDS == excludes) {
      return includes;
    }

    if (SELECT_ALL_FIELDS == excludes) {
      return SELECT_NO_FIELDS;
    }
    
    if (SELECT_ALL_FIELDS == includes) {
      return fieldName -> ! excludes.shouldMutate(fieldName);
    }

    return fieldName -> (includes.shouldMutate(fieldName)
            && ! excludes.shouldMutate(fieldName));
  }

  /**
   * Utility method that can be used to define a FieldNameSelector
   * using the same types of rules as the FieldMutatingUpdateProcessor init
   * code.  This may be useful for Factories that wish to define default
   * selectors in similar terms to what the configuration would look like.
   * @lucene.internal
   */
  public static FieldNameSelector createFieldNameSelector
    (final SolrResourceLoader loader,
     final SolrCore core,
     final SelectorParams params,
     final FieldNameSelector defSelector) {

    if (params.noSelectorsSpecified()) {
      return defSelector;
    }

    final ConfigurableFieldNameSelectorHelper helper =
      new ConfigurableFieldNameSelectorHelper(loader, params);
    return fieldName -> helper.shouldMutateBasedOnSchema(fieldName, core.getLatestSchema());
  }

  /**
   * Utility method that can be used to define a FieldNameSelector
   * using the same types of rules as the FieldMutatingUpdateProcessor init 
   * code.  This may be useful for Factories that wish to define default 
   * selectors in similar terms to what the configuration would look like.
   * Uses {@code schema} for checking field existence.
   * @lucene.internal
   */
  public static FieldNameSelector createFieldNameSelector
    (final SolrResourceLoader loader,
     final IndexSchema schema,
     final SelectorParams params,
     final FieldNameSelector defSelector) {

    if (params.noSelectorsSpecified()) {
      return defSelector;
    }

    final ConfigurableFieldNameSelectorHelper helper =
      new ConfigurableFieldNameSelectorHelper(loader, params);
    return fieldName -> helper.shouldMutateBasedOnSchema(fieldName, schema);
  }
  
  private static final class ConfigurableFieldNameSelectorHelper {

    final SelectorParams params;
    @SuppressWarnings({"rawtypes"})
    final Collection<Class> classes;

    private ConfigurableFieldNameSelectorHelper(final SolrResourceLoader loader,
                                          final SelectorParams params) {
      this.params = params;

      @SuppressWarnings({"rawtypes"})
      final Collection<Class> classes = new ArrayList<>(params.typeClass.size());

      for (String t : params.typeClass) {
        try {
          classes.add(loader.findClass(t, Object.class));
        } catch (Exception e) {
          throw new SolrException(SERVER_ERROR, "Can't resolve typeClass: " + t, e);
        }
      }
      this.classes = classes;
    }

    public boolean shouldMutateBasedOnSchema(final String fieldName, IndexSchema schema) {
      // order of checks is based on what should be quicker
      // (ie: set lookups faster the looping over instanceOf / matches tests
      
      if ( ! (params.fieldName.isEmpty() || params.fieldName.contains(fieldName)) ) {
        return false;
      }
      
      // do not consider it an error if the fieldName has no type
      // there might be another processor dealing with it later
      FieldType t =  schema.getFieldTypeNoEx(fieldName);
      final boolean fieldExists = (null != t);

      if ( (null != params.fieldNameMatchesSchemaField) &&
           (fieldExists != params.fieldNameMatchesSchemaField) ) {
        return false;
      }

      if (fieldExists) { 

        if (! (params.typeName.isEmpty() || params.typeName.contains(t.getTypeName())) ) {
          return false;
        }
        
        if (! (classes.isEmpty() || instanceOfAny(t, classes)) ) {
          return false;
        }
      } 
      
      if (! (params.fieldRegex.isEmpty() || matchesAny(fieldName, params.fieldRegex)) ) {
        return false;
      }
      
      return true;
    }

    /**
     * returns true if the Object 'o' is an instance of any class in 
     * the Collection
     */
    private static boolean instanceOfAny(Object o,
                                         @SuppressWarnings({"rawtypes"})Collection<Class> classes) {
      for (@SuppressWarnings({"rawtypes"})Class c : classes) {
        if ( c.isInstance(o) ) return true;
      }
      return false;
    }
    
    /**
     * returns true if the CharSequence 's' matches any Pattern in the 
     * Collection
     */
    private static boolean matchesAny(CharSequence s, 
                                      Collection<Pattern> regexes) {
      for (Pattern p : regexes) {
        if (p.matcher(s).matches()) return true;
      }
      return false;
    }
   }
  public static FieldMutatingUpdateProcessor mutator(FieldNameSelector selector,
                                              UpdateRequestProcessor next,
                                              Function<SolrInputField,SolrInputField> fun){
    return new FieldMutatingUpdateProcessor(selector, next) {
      @Override
      protected SolrInputField mutate(SolrInputField src) {
        return fun.apply(src);
      }
    };

  }
}

