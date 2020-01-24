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

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessorFactory.SelectorParams;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clones the values found in any matching <code>source</code> field into 
 * a configured <code>dest</code> field.
 * <p>
 * The <code>source</code> field(s) can be configured as either:
 * </p>
 * <ul>
 *  <li>One or more <code>&lt;str&gt;</code></li>
 *  <li>An <code>&lt;arr&gt;</code> of <code>&lt;str&gt;</code></li>
 *  <li>A <code>&lt;lst&gt;</code> containing {@link FieldMutatingUpdateProcessorFactory FieldMutatingUpdateProcessorFactory style selector arguments}</li>
 * </ul>
 *
 * <p> The <code>dest</code> field can be a single <code>&lt;str&gt;</code> 
 * containing the literal name of a destination field, or it may be a <code>&lt;lst&gt;</code> specifying a 
 * regex <code>pattern</code> and a <code>replacement</code> string. If the pattern + replacement option 
 * is used the pattern will be matched against all fields matched by the source selector, and the replacement 
 * string (including any capture groups specified from the pattern) will be evaluated a using 
 * {@link Matcher#replaceAll(String)} to generate the literal name of the destination field.
 * </p>
 *
 * <p>If the resolved <code>dest</code> field already exists in the document, then the 
 * values from the <code>source</code> fields will be added to it.  The 
 * "boost" value associated with the <code>dest</code> will not be changed, 
 * and any boost specified on the <code>source</code> fields will be ignored.  
 * (If the <code>dest</code> field did not exist prior to this processor, the 
 * newly created <code>dest</code> field will have the default boost of 1.0)
 * </p>
 * <p>
 * In the example below:
 * </p>
 * <ul>
 *   <li>The <code>category</code> field will be cloned into the <code>category_s</code> field</li>
 *   <li>Both the <code>authors</code> and <code>editors</code> fields will be cloned into the 
 *       <code>contributors</code> field
 *   </li>
 *   <li>Any field with a name ending in <code>_price</code> -- except for 
 *       <code>list_price</code> -- will be cloned into the <code>all_prices</code>
 *   </li>
 *   <li>Any field name beginning with feat and ending in s (i.e. feats or features) 
 *       will be cloned into a field prefixed with key_ and not ending in s. (i.e. key_feat or key_feature)
 *   </li>
 * </ul>
 *
 * <!-- see solrconfig-update-processors-chains.xml and 
 *      CloneFieldUpdateProcessorFactoryTest.testCloneFieldExample for where this is tested -->
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
 *         &lt;str name="fieldRegex"&gt;.*_price$&lt;/str&gt;
 *         &lt;lst name="exclude"&gt;
 *           &lt;str name="fieldName"&gt;list_price&lt;/str&gt;
 *         &lt;/lst&gt;
 *       &lt;/lst&gt;
 *       &lt;str name="dest"&gt;all_prices&lt;/str&gt;
 *     &lt;/processor&gt;
 *     &lt;processor class="solr.processor.CloneFieldUpdateProcessorFactory"&gt;
 *       &lt;lst name="source"&gt;
 *         &lt;str name="fieldRegex"&gt;^feat(.*)s$&lt;/str&gt;
 *       &lt;/lst&gt;
 *       &lt;lst name="dest"&gt;
 *         &lt;str name="pattern"&gt;^feat(.*)s$&lt;/str&gt;
 *         &lt;str name="replacement"&gt;key_feat$1&lt;/str&gt;
 *       &lt;/str&gt;
 *     &lt;/processor&gt;
 *   &lt;/updateRequestProcessorChain&gt;
 * </pre>
 *
 * <p>
 * In common case situations where you wish to use a single regular expression as both a 
 * <code>fieldRegex</code> selector and a destination <code>pattern</code>, a "short hand" syntax 
 * is support for convinience: The <code>pattern</code> and <code>replacement</code> may be specified 
 * at the top level, omitting <code>source</code> and <code>dest</code> declarations completely, and 
 * the <code>pattern</code> will be used to construct an equivalent <code>source</code> selector internally.
 * </p>
 * <p>
 * For example, both of the following configurations are equivalent:
 * </p>
 * <pre class="prettyprint">
 * &lt;!-- full syntax --&gt;
 * &lt;processor class="solr.processor.CloneFieldUpdateProcessorFactory"&gt;
 *   &lt;lst name="source"&gt;
 *     &lt;str name="fieldRegex"^gt;$feat(.*)s$&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="dest"&gt;
 *     &lt;str name="pattern"&gt;^feat(.*)s$&lt;/str&gt;
 *     &lt;str name="replacement"&gt;key_feat$1&lt;/str&gt;
 *   &lt;/str&gt;
 * &lt;/processor&gt;
 * 
 * &lt;!-- syntactic sugar syntax --&gt;
 * &lt;processor class="solr.processor.CloneFieldUpdateProcessorFactory"&gt;
 *   &lt;str name="pattern"&gt;^feat(.*)s$&lt;/str&gt;
 *   &lt;str name="replacement"&gt;key_feat$1&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 * <p>
 * When cloning multiple fields (or a single multivalued field) into a single valued field, one of the 
 * {@link FieldValueSubsetUpdateProcessorFactory} implementations configured after the 
 * <code>CloneFieldUpdateProcessorFactory</code> can be useful to reduce the list of values down to a 
 * single value.
 * </p>
 * 
 * @see FieldValueSubsetUpdateProcessorFactory
 * @since 4.0.0
 */
public class CloneFieldUpdateProcessorFactory 
  extends UpdateRequestProcessorFactory implements SolrCoreAware {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final String SOURCE_PARAM = "source";
  public static final String DEST_PARAM = "dest";
  public static final String PATTERN_PARAM = "pattern";
  public static final String REPLACEMENT_PARAM = "replacement";

  private SelectorParams srcInclusions = new SelectorParams();
  private Collection<SelectorParams> srcExclusions 
    = new ArrayList<>();

  private FieldNameSelector srcSelector = null;

  /** 
   * If pattern is null, this this is a literal field name.  If pattern is non-null then this
   * is a replacement string that may contain meta-characters (ie: capture group identifiers)
   * @see #pattern
   */
  private String dest = null;
  /** @see #dest */
  private Pattern pattern = null;

  @SuppressWarnings("WeakerAccess")
  protected final FieldNameSelector getSourceSelector() {
    if (null != srcSelector) return srcSelector;

    throw new SolrException(SERVER_ERROR, "selector was never initialized, "+
                            " inform(SolrCore) never called???");
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(NamedList args) {

    // high level (loose) check for which type of config we have.
    // 
    // individual init methods do more strict syntax checking
    if (0 <= args.indexOf(SOURCE_PARAM, 0) && 0 <= args.indexOf(DEST_PARAM, 0) ) {
      initSourceSelectorSyntax(args);
    } else if (0 <= args.indexOf(PATTERN_PARAM, 0) && 0 <= args.indexOf(REPLACEMENT_PARAM, 0)) {
      initSimpleRegexReplacement(args);
    } else {
      throw new SolrException(SERVER_ERROR, "A combination of either '" + SOURCE_PARAM + "' + '"+
                              DEST_PARAM + "', or '" + REPLACEMENT_PARAM + "' + '" +
                              PATTERN_PARAM + "' init params are mandatory");
    }
    
    if (0 < args.size()) {
      throw new SolrException(SERVER_ERROR,
          "Unexpected init param(s): '" +
              args.getName(0) + "'");
    }

    super.init(args);
  }

  /**
   * init helper method that should only be called when we know for certain that both the 
   * "source" and "dest" init params do <em>not</em> exist.
   */
  @SuppressWarnings("unchecked")
  private void initSimpleRegexReplacement(NamedList args) {
    // The syntactic sugar for the case where there is only one regex pattern for source and the same pattern
    // is used for the destination pattern...
    //
    //  pattern != null && replacement != null
    //    
    // ...as top level elements, with no other config options specified
    
    // if we got here we know we had pattern and replacement, now check for the other two  so that we can give a better
    // message than "unexpected"
    if (0 <= args.indexOf(SOURCE_PARAM, 0) || 0 <= args.indexOf(DEST_PARAM, 0) ) {
      throw new SolrException(SERVER_ERROR,"Short hand syntax must not be mixed with full syntax. Found " + 
          PATTERN_PARAM + " and " + REPLACEMENT_PARAM + " but also found " + SOURCE_PARAM + " or " + DEST_PARAM);
    }
    
    assert args.indexOf(SOURCE_PARAM, 0) < 0;
    
    Object patt = args.remove(PATTERN_PARAM);
    Object replacement = args.remove(REPLACEMENT_PARAM);

    if (null == patt || null == replacement) {
      throw new SolrException(SERVER_ERROR, "Init params '" + PATTERN_PARAM + "' and '" +
                              REPLACEMENT_PARAM + "' are both mandatory if '" + SOURCE_PARAM + "' and '"+
                              DEST_PARAM + "' are not both specified");
    }

    if (0 != args.size()) {
      throw new SolrException(SERVER_ERROR, "Init params '" + REPLACEMENT_PARAM + "' and '" +
                              PATTERN_PARAM + "' must be children of '" + DEST_PARAM +
                              "' to be combined with other options.");
    }
    
    if (!(replacement instanceof String)) {
      throw new SolrException(SERVER_ERROR, "Init param '" + REPLACEMENT_PARAM + "' must be a string (i.e. <str>)");
    }
    if (!(patt instanceof String)) {
      throw new SolrException(SERVER_ERROR, "Init param '" + PATTERN_PARAM + "' must be a string (i.e. <str>)");
    }
    
    dest = replacement.toString();
    try {
      this.pattern = Pattern.compile(patt.toString());
    } catch (PatternSyntaxException pe) {
      throw new SolrException(SERVER_ERROR, "Init param " + PATTERN_PARAM +
                              " is not a valid regex pattern: " + patt, pe);
      
    }
    srcInclusions = new SelectorParams();
    srcInclusions.fieldRegex = Collections.singletonList(this.pattern);
  }
 
  /**
   * init helper method that should only be called when we know for certain that both the 
   * "source" and "dest" init params <em>do</em> exist.
   */
  @SuppressWarnings("unchecked")
  private void initSourceSelectorSyntax(NamedList args) {
    // Full and complete syntax where source and dest are mandatory.
    //
    // source may be a single string or a selector.
    // dest may be a single string or list containing pattern and replacement
    //
    //   source != null && dest != null

    // if we got here we know we had source and dest, now check for the other two so that we can give a better
    // message than "unexpected"
    if (0 <= args.indexOf(PATTERN_PARAM, 0) || 0 <= args.indexOf(REPLACEMENT_PARAM, 0) ) {
      throw new SolrException(SERVER_ERROR,"Short hand syntax must not be mixed with full syntax. Found " +
          SOURCE_PARAM + " and " + DEST_PARAM + " but also found " + PATTERN_PARAM + " or " + REPLACEMENT_PARAM);
    }

    Object d = args.remove(DEST_PARAM);
    assert null != d;
    
    List<Object> sources = args.getAll(SOURCE_PARAM);
    assert null != sources;

    if (1 == sources.size()) {
      if (sources.get(0) instanceof NamedList) {
        // nested set of selector options
        NamedList selectorConfig = (NamedList) args.remove(SOURCE_PARAM);

        srcInclusions = parseSelectorParams(selectorConfig);

        List<Object> excList = selectorConfig.getAll("exclude");

        for (Object excObj : excList) {
          if (null == excObj) {
            throw new SolrException(SERVER_ERROR, "Init param '" + SOURCE_PARAM +
                                    "' child 'exclude' can not be null");
          }
          if (!(excObj instanceof NamedList)) {
            throw new SolrException(SERVER_ERROR, "Init param '" + SOURCE_PARAM +
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
        // consume from the named list so it doesn't interfere with subsequent processing
        sources.remove(0);
      }
    }
    if (1 <= sources.size()) {
      // source better be one or more strings
      srcInclusions.fieldName = new HashSet<>(args.removeConfigArgs("source"));
    }
    if (srcInclusions == null) {
      throw new SolrException(SERVER_ERROR, "Init params do not specify anything to clone, please supply either "
      + SOURCE_PARAM + " and " + DEST_PARAM + " or " + PATTERN_PARAM + " and " + REPLACEMENT_PARAM + ". See javadocs" +
          "for CloneFieldUpdateProcessorFactory for further details.");
    }
    
    if (d instanceof NamedList) {
      NamedList destList = (NamedList) d;

      Object patt = destList.remove(PATTERN_PARAM);
      Object replacement = destList.remove(REPLACEMENT_PARAM);
      
      if (null == patt || null == replacement) {
        throw new SolrException(SERVER_ERROR, "Init param '" + DEST_PARAM + "' children '" +
                                PATTERN_PARAM + "' and '" + REPLACEMENT_PARAM +
                                "' are both mandatoryand can not be null");
      }
      if (! (patt instanceof String && replacement instanceof String)) {
        throw new SolrException(SERVER_ERROR, "Init param '" + DEST_PARAM + "' children '" +
                                PATTERN_PARAM + "' and '" + REPLACEMENT_PARAM +
                                "' must both be strings (i.e. <str>)");
      }
      if (0 != destList.size()) {
        throw new SolrException(SERVER_ERROR, "Init param '" + DEST_PARAM + "' has unexpected children: '"
                                + destList.getName(0) + "'");
      }
      
      try {
        this.pattern = Pattern.compile(patt.toString());
      } catch (PatternSyntaxException pe) {
        throw new SolrException(SERVER_ERROR, "Init param '" + DEST_PARAM + "' child '" + PATTERN_PARAM +
                                " is not a valid regex pattern: " + patt, pe);
      }
      dest = replacement.toString();
        
    } else if (d instanceof String) {
      dest = d.toString();
    } else {
      throw new SolrException(SERVER_ERROR, "Init param '" + DEST_PARAM + "' must either be a string " +
                              "(i.e. <str>) or a list (i.e. <lst>) containing '" +
                              PATTERN_PARAM + "' and '" + REPLACEMENT_PARAM);
    }

  }

  @Override
  public void inform(final SolrCore core) {
    
    srcSelector = 
      FieldMutatingUpdateProcessor.createFieldNameSelector
          (core.getResourceLoader(), core, srcInclusions, FieldMutatingUpdateProcessor.SELECT_NO_FIELDS);

    for (SelectorParams exc : srcExclusions) {
      srcSelector = FieldMutatingUpdateProcessor.wrap
        (srcSelector,
         FieldMutatingUpdateProcessor.createFieldNameSelector
             (core.getResourceLoader(), core, exc, FieldMutatingUpdateProcessor.SELECT_NO_FIELDS));
    }
  }

  @Override
  public final UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                                  SolrQueryResponse rsp,
                                                  UpdateRequestProcessor next) {
    final FieldNameSelector srcSelector = getSourceSelector();
    return new UpdateRequestProcessor(next) {
      @Override
      public void processAdd(AddUpdateCommand cmd) throws IOException {

        final SolrInputDocument doc = cmd.getSolrInputDocument();

        // destination may be regex replace string, which can cause multiple output fields.
        Map<String,SolrInputField> destMap = new HashMap<>();

        // preserve initial values and boost (if any)
        for (final String fname : doc.getFieldNames()) {
          if (! srcSelector.shouldMutate(fname)) continue;
          
          Collection<Object> srcFieldValues = doc.getFieldValues(fname);
          if(srcFieldValues == null || srcFieldValues.isEmpty()) continue;
          
          String resolvedDest = dest;

          if (pattern != null) {
            Matcher matcher = pattern.matcher(fname);
            if (matcher.find()) {
              resolvedDest = matcher.replaceAll(dest);
            } else {
              log.debug("CloneFieldUpdateProcessor.srcSelector.shouldMutate(\"{}\") returned true, " +
                  "but replacement pattern did not match, field skipped.", fname);
              continue;
            }
          }
          SolrInputField destField;
          if (doc.containsKey(resolvedDest)) {
            destField = doc.getField(resolvedDest);
          } else {
            SolrInputField targetField = destMap.get(resolvedDest);
            if (targetField == null) {
              destField = new SolrInputField(resolvedDest);
            } else {
              destField = targetField;
            }
          }

          for (Object val : srcFieldValues) {
            destField.addValue(val);
          }
          // put it in map to avoid concurrent modification...
          destMap.put(resolvedDest, destField);
        }

        for (Map.Entry<String, SolrInputField> entry : destMap.entrySet()) {
          doc.put(entry.getKey(), entry.getValue());
        }
        super.processAdd(cmd);
      }
    };
  }

  /** macro */
  private static SelectorParams parseSelectorParams(NamedList args) {
    return FieldMutatingUpdateProcessorFactory.parseSelectorParams(args);
  }

}
