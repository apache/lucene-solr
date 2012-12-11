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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;

import org.apache.solr.schema.IndexSchema;

import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.plugin.SolrCoreAware;

import org.apache.solr.common.util.NamedList;

import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.SolrInputDocument;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import static org.apache.solr.common.SolrException.ErrorCode.*;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import org.apache.solr.update.AddUpdateCommand;

import org.apache.solr.update.processor.FieldMutatingUpdateProcessorFactory;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessorFactory.SelectorParams;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clones the values found in any matching <code>source</code> field into 
 * the configured <code>dest</code> field.
 * <p>
 * While the <code>dest</code> field must be a single <code>&lt;str&gt;</code>, 
 * the <code>source</code> fields can be configured as either:
 * </p>
 * <ul>
 *  <li>One or more <code>&lt;str&gt;</code></li>
 *  <li>An <code>&lt;arr&gt;</code> of <code>&lt;str&gt;</code></li>
 *  <li>A <code>&lt;lst&gt;</code> containing {@link FieldMutatingUpdateProcessorFactory FieldMutatingUpdateProcessorFactory style selector arguments}</li>
 * </ul>
 * <p>
 * If the <code>dest</code> field already exists in the document, then the 
 * values from the <code>source</code> fields will be added to it.  The 
 * "boost" value associated with the <code>dest</code> will not be changed, 
 * and any boost specified on the <code>source</code> fields will be ignored.  
 * (If the <code>dest</code> field did not exist prior to this processor, the 
 * newly created <code>dest</code> field will have the default boost of 1.0)
 * </p>
 * <p>
 * In the example below, the <code>category</code> field will be cloned 
 * into the <code>category_s</code> field, both the <code>authors</code> and 
 * <code>editors</code> fields will be cloned into the <code>contributors</code>
 * field, and any field with a name ending in <code>_price</code> -- except for 
 * <code>list_price</code> -- will be cloned into the <code>all_prices</code> 
 * field. 
 * </p>
 * <!-- see solrconfig-update-processors-chains.xml for where this is tested -->
 * <pre class="prettyprint">
 *   &lt;updateRequestProcessorChain name="multiple-clones"&gt;
 *     &lt;processor class="solr.CloneFieldUpdateProcessorFactory"&gt;
 *       &lt;str name="source"&gt;category&lt;/str&gt;
 *       &lt;str name="dest"&gt;category_s&lt;/str&gt;
 *     &lt;/processor&gt;
 *     &lt;processor class="solr.CloneFieldUpdateProcessorFactory"&gt;
 *       &lt;arr name="source"&gt;
 *         &lt;str&gt;authors&lt;/str&gt;
 *         &lt;str&gt;editors&lt;/str&gt;
 *       &lt;/arr&gt;
 *       &lt;str name="dest"&gt;contributors&lt;/str&gt;
 *     &lt;/processor&gt;
 *     &lt;processor class="solr.CloneFieldUpdateProcessorFactory"&gt;
 *       &lt;lst name="source"&gt;
 *         &lt;str name="fieldRegex"&gt;.*_price&lt;/str&gt;
 *         &lt;lst name="exclude"&gt;
 *           &lt;str name="fieldName"&gt;list_price&lt;/str&gt;
 *         &lt;/lst&gt;
 *       &lt;/lst&gt;
 *       &lt;str name="dest"&gt;all_prices&lt;/str&gt;
 *     &lt;/processor&gt;
 *   &lt;/updateRequestProcessorChain&gt;
 * </pre>
 */
public class CloneFieldUpdateProcessorFactory 
  extends UpdateRequestProcessorFactory implements SolrCoreAware {
  
  private final static Logger log = LoggerFactory.getLogger(CloneFieldUpdateProcessorFactory.class);
  
  public static final String SOURCE_PARAM = "source";
  public static final String DEST_PARAM = "dest";
  
  private SelectorParams srcInclusions = new SelectorParams();
  private Collection<SelectorParams> srcExclusions 
    = new ArrayList<SelectorParams>();

  private FieldNameSelector srcSelector = null;
  private String dest = null;

  protected final FieldNameSelector getSourceSelector() {
    if (null != srcSelector) return srcSelector;

    throw new SolrException(SERVER_ERROR, "selector was never initialized, "+
                            " inform(SolrCore) never called???");
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(NamedList args) {
    Object d = args.remove(DEST_PARAM);
    if (null == d) {
      throw new SolrException
        (SERVER_ERROR, "Init param '" + DEST_PARAM + "' must be specified"); 
    } else if (! (d instanceof CharSequence) ) {
      throw new SolrException
        (SERVER_ERROR, "Init param '" + DEST_PARAM + "' must be a string (ie: 'str')");
    }
    dest = d.toString();

    List<Object> sources = args.getAll(SOURCE_PARAM);
    if (0 == sources.size()) {
      throw new SolrException
        (SERVER_ERROR, "Init param '" + SOURCE_PARAM + "' must be specified"); 
    } 
    if (1 == sources.size() && sources.get(0) instanceof NamedList) {
      // nested set of selector options
      NamedList selectorConfig = (NamedList) args.remove(SOURCE_PARAM);

      srcInclusions = parseSelectorParams(selectorConfig);

      List<Object> excList = selectorConfig.getAll("exclude");

      for (Object excObj : excList) {
        if (null == excObj) {
          throw new SolrException
            (SERVER_ERROR, "Init param '" + SOURCE_PARAM + 
             "' child 'exclude' can not be null"); 
        }
        if (! (excObj instanceof NamedList) ) {
          throw new SolrException
            (SERVER_ERROR, "Init param '" + SOURCE_PARAM + 
             "' child 'exclude' must be <lst/>"); 
        }
        NamedList exc = (NamedList) excObj;
        srcExclusions.add(parseSelectorParams(exc));
        if (0 < exc.size()) {
          throw new SolrException(SERVER_ERROR, "Init param '" + SOURCE_PARAM + 
                                  "' has unexpected 'exclude' sub-param(s): '" 
                                  + selectorConfig.getName(0) + "'");
        }
        // call once per instance
        selectorConfig.remove("exclude");
      }

      if (0 < selectorConfig.size()) {
        throw new SolrException(SERVER_ERROR, "Init param '" + SOURCE_PARAM + 
                                "' contains unexpected child param(s): '" + 
                                selectorConfig.getName(0) + "'");
      }
    } else {
      // source better be one or more strings
      srcInclusions.fieldName = new HashSet<String>
        (FieldMutatingUpdateProcessorFactory.oneOrMany(args, "source"));
    }

    

    if (0 < args.size()) {
      throw new SolrException(SERVER_ERROR, 
                              "Unexpected init param(s): '" + 
                              args.getName(0) + "'");
    }

    super.init(args);
  }

  @Override
  public void inform(final SolrCore core) {
    
    final IndexSchema schema = core.getSchema();

    srcSelector = 
      FieldMutatingUpdateProcessor.createFieldNameSelector
      (core.getResourceLoader(),
       core.getSchema(),
       srcInclusions.fieldName,
       srcInclusions.typeName,
       srcInclusions.typeClass,
       srcInclusions.fieldRegex,
       FieldMutatingUpdateProcessor.SELECT_NO_FIELDS);

    for (SelectorParams exc : srcExclusions) {
      srcSelector = FieldMutatingUpdateProcessor.wrap
        (srcSelector,
         FieldMutatingUpdateProcessor.createFieldNameSelector
         (core.getResourceLoader(),
          core.getSchema(),
          exc.fieldName,
          exc.typeName,
          exc.typeClass,
          exc.fieldRegex,
          FieldMutatingUpdateProcessor.SELECT_NO_FIELDS));
    }
  }

  @Override
  public final UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                                  SolrQueryResponse rsp,
                                                  UpdateRequestProcessor next) {
    return new UpdateRequestProcessor(next) {
      @Override
      public void processAdd(AddUpdateCommand cmd) throws IOException {

        final SolrInputDocument doc = cmd.getSolrInputDocument();

        // preserve initial values and boost (if any)
        SolrInputField destField = doc.containsKey(dest) ? 
          doc.getField(dest) : new SolrInputField(dest); 
        
        boolean modified = false;
        for (final String fname : doc.getFieldNames()) {
          if (! srcSelector.shouldMutate(fname)) continue;

          for (Object val : doc.getFieldValues(fname)) {
            // preserve existing dest boost (multiplicitive), ignore src boost
            destField.addValue(val, 1.0f);
          }
          modified=true;
        }

        if (modified) doc.put(dest, destField);

        super.processAdd(cmd);
      }
    };
  }

  /** macro */
  private static SelectorParams parseSelectorParams(NamedList args) {
    return FieldMutatingUpdateProcessorFactory.parseSelectorParams(args);
  }

}
