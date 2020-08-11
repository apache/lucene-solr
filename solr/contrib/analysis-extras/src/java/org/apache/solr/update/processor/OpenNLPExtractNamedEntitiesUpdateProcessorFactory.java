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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import opennlp.tools.util.Span;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.opennlp.OpenNLPTokenizer;
import org.apache.lucene.analysis.opennlp.tools.NLPNERTaggerOp;
import org.apache.lucene.analysis.opennlp.tools.OpenNLPOpsFactory;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessor.FieldNameSelector;
import org.apache.solr.update.processor.FieldMutatingUpdateProcessorFactory.SelectorParams;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

/**
 * Extracts named entities using an OpenNLP NER <code>modelFile</code> from the values found in
 * any matching <code>source</code> field into a configured <code>dest</code> field, after
 * first tokenizing the source text using the index analyzer on the configured
 * <code>analyzerFieldType</code>, which must include <code>solr.OpenNLPTokenizerFactory</code>
 * as the tokenizer. E.g.:
 *
 * <pre class="prettyprint">
 *   &lt;fieldType name="opennlp-en-tokenization" class="solr.TextField"&gt;
 *     &lt;analyzer&gt;
 *       &lt;tokenizer class="solr.OpenNLPTokenizerFactory"
 *                  sentenceModel="en-sent.bin"
 *                  tokenizerModel="en-tokenizer.bin"/&gt;
 *     &lt;/analyzer&gt;
 *   &lt;/fieldType&gt;
 * </pre>
 * 
 * <p>See the <a href="http://opennlp.apache.org/models.html">OpenNLP website</a>
 * for information on downloading pre-trained models.</p>
 *
 * Note that in order to use model files larger than 1MB on SolrCloud, 
 * <a href="https://lucene.apache.org/solr/guide/setting-up-an-external-zookeeper-ensemble#increasing-zookeeper-s-1mb-file-size-limit"
 * >ZooKeeper server and client configuration is required</a>.
 * 
 * <p>
 * The <code>source</code> field(s) can be configured as either:
 * </p>
 * <ul>
 *  <li>One or more <code>&lt;str&gt;</code></li>
 *  <li>An <code>&lt;arr&gt;</code> of <code>&lt;str&gt;</code></li>
 *  <li>A <code>&lt;lst&gt;</code> containing
 *   {@link FieldMutatingUpdateProcessor FieldMutatingUpdateProcessorFactory style selector arguments}</li>
 * </ul>
 *
 * <p>The <code>dest</code> field can be a single <code>&lt;str&gt;</code>
 * containing the literal name of a destination field, or it may be a <code>&lt;lst&gt;</code> specifying a
 * regex <code>pattern</code> and a <code>replacement</code> string. If the pattern + replacement option
 * is used the pattern will be matched against all fields matched by the source selector, and the replacement
 * string (including any capture groups specified from the pattern) will be evaluated a using
 * {@link Matcher#replaceAll(String)} to generate the literal name of the destination field.  Additionally,
 * an occurrence of the string "{EntityType}" in the <code>dest</code> field specification, or in the
 * <code>replacement</code> string, will be replaced with the entity type(s) returned for each entity by
 * the OpenNLP NER model; as a result, if the model extracts more than one entity type, then more than one
 * <code>dest</code> field will be populated.
 * </p>
 *
 * <p>If the resolved <code>dest</code> field already exists in the document, then the
 * named entities extracted from the <code>source</code> fields will be added to it.
 * </p>
 * <p>
 * In the example below:
 * </p>
 * <ul>
 *   <li>Named entities will be extracted from the <code>text</code> field and added
 *       to the <code>names_ss</code> field</li>
 *   <li>Named entities will be extracted from both the <code>title</code> and
 *       <code>subtitle</code> fields and added into the <code>titular_people</code> field</li>
 *   <li>Named entities will be extracted from any field with a name ending in <code>_txt</code>
 *       -- except for <code>notes_txt</code> -- and added into the <code>people_ss</code> field</li>
 *   <li>Named entities will be extracted from any field with a name beginning with "desc" and
 *       ending in "s" (e.g. "descs" and "descriptions") and added to a field prefixed with "key_",
 *       not ending in "s", and suffixed with "_people". (e.g. "key_desc_people" or
 *       "key_description_people")</li>
 *   <li>Named entities will be extracted from the <code>summary</code> field and added
 *       to the <code>summary_person_ss</code> field, assuming that the modelFile only extracts
 *       entities of type "person".</li>
 * </ul>
 *
 * <pre class="prettyprint">
 * &lt;updateRequestProcessorChain name="multiple-extract"&gt;
 *   &lt;processor class="solr.OpenNLPExtractNamedEntitiesUpdateProcessorFactory"&gt;
 *     &lt;str name="modelFile"&gt;en-test-ner-person.bin&lt;/str&gt;
 *     &lt;str name="analyzerFieldType"&gt;opennlp-en-tokenization&lt;/str&gt;
 *     &lt;str name="source"&gt;text&lt;/str&gt;
 *     &lt;str name="dest"&gt;people_s&lt;/str&gt;
 *   &lt;/processor&gt;
 *   &lt;processor class="solr.OpenNLPExtractNamedEntitiesUpdateProcessorFactory"&gt;
 *     &lt;str name="modelFile"&gt;en-test-ner-person.bin&lt;/str&gt;
 *     &lt;str name="analyzerFieldType"&gt;opennlp-en-tokenization&lt;/str&gt;
 *     &lt;arr name="source"&gt;
 *       &lt;str&gt;title&lt;/str&gt;
 *       &lt;str&gt;subtitle&lt;/str&gt;
 *     &lt;/arr&gt;
 *     &lt;str name="dest"&gt;titular_people&lt;/str&gt;
 *   &lt;/processor&gt;
 *   &lt;processor class="solr.OpenNLPExtractNamedEntitiesUpdateProcessorFactory"&gt;
 *     &lt;str name="modelFile"&gt;en-test-ner-person.bin&lt;/str&gt;
 *     &lt;str name="analyzerFieldType"&gt;opennlp-en-tokenization&lt;/str&gt;
 *     &lt;lst name="source"&gt;
 *       &lt;str name="fieldRegex"&gt;.*_txt$&lt;/str&gt;
 *       &lt;lst name="exclude"&gt;
 *         &lt;str name="fieldName"&gt;notes_txt&lt;/str&gt;
 *       &lt;/lst&gt;
 *     &lt;/lst&gt;
 *     &lt;str name="dest"&gt;people_s&lt;/str&gt;
 *   &lt;/processor&gt;
 *   &lt;processor class="solr.processor.OpenNLPExtractNamedEntitiesUpdateProcessorFactory"&gt;
 *     &lt;str name="modelFile"&gt;en-test-ner-person.bin&lt;/str&gt;
 *     &lt;str name="analyzerFieldType"&gt;opennlp-en-tokenization&lt;/str&gt;
 *     &lt;lst name="source"&gt;
 *       &lt;str name="fieldRegex"&gt;^desc(.*)s$&lt;/str&gt;
 *     &lt;/lst&gt;
 *     &lt;lst name="dest"&gt;
 *       &lt;str name="pattern"&gt;^desc(.*)s$&lt;/str&gt;
 *       &lt;str name="replacement"&gt;key_desc$1_people&lt;/str&gt;
 *     &lt;/lst&gt;
 *   &lt;/processor&gt;
 *   &lt;processor class="solr.OpenNLPExtractNamedEntitiesUpdateProcessorFactory"&gt;
 *     &lt;str name="modelFile"&gt;en-test-ner-person.bin&lt;/str&gt;
 *     &lt;str name="analyzerFieldType"&gt;opennlp-en-tokenization&lt;/str&gt;
 *     &lt;str name="source"&gt;summary&lt;/str&gt;
 *     &lt;str name="dest"&gt;summary_{EntityType}_s&lt;/str&gt;
 *   &lt;/processor&gt;
 *   &lt;processor class="solr.LogUpdateProcessorFactory" /&gt;
 *   &lt;processor class="solr.RunUpdateProcessorFactory" /&gt;
 * &lt;/updateRequestProcessorChain&gt;
 * </pre>
 *
 * @since 7.3.0
 */
public class OpenNLPExtractNamedEntitiesUpdateProcessorFactory
    extends UpdateRequestProcessorFactory implements SolrCoreAware {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SOURCE_PARAM = "source";
  public static final String DEST_PARAM = "dest";
  public static final String PATTERN_PARAM = "pattern";
  public static final String REPLACEMENT_PARAM = "replacement";
  public static final String MODEL_PARAM = "modelFile";
  public static final String ANALYZER_FIELD_TYPE_PARAM = "analyzerFieldType";
  public static final String ENTITY_TYPE = "{EntityType}";

  private SelectorParams srcInclusions = new SelectorParams();
  private Collection<SelectorParams> srcExclusions = new ArrayList<>();

  private FieldNameSelector srcSelector = null;

  private String modelFile = null;
  private String analyzerFieldType = null;

  /**
   * If pattern is null, this this is a literal field name.  If pattern is non-null then this
   * is a replacement string that may contain meta-characters (ie: capture group identifiers)
   * @see #pattern
   */
  private String dest = null;
  /** @see #dest */
  private Pattern pattern = null;

  protected final FieldNameSelector getSourceSelector() {
    if (null != srcSelector) return srcSelector;

    throw new SolrException(SERVER_ERROR, "selector was never initialized, inform(SolrCore) never called???");
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {

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

    Object modelParam = args.remove(MODEL_PARAM);
    if (null == modelParam) {
      throw new SolrException(SERVER_ERROR, "Missing required init param '" + MODEL_PARAM + "'");
    }
    if ( ! (modelParam instanceof CharSequence)) {
      throw new SolrException(SERVER_ERROR, "Init param '" + MODEL_PARAM + "' must be a <str>");
    }
    modelFile = modelParam.toString();

    Object analyzerFieldTypeParam = args.remove(ANALYZER_FIELD_TYPE_PARAM);
    if (null == analyzerFieldTypeParam) {
      throw new SolrException(SERVER_ERROR, "Missing required init param '" + ANALYZER_FIELD_TYPE_PARAM + "'");
    }
    if ( ! (analyzerFieldTypeParam instanceof CharSequence)) {
      throw new SolrException(SERVER_ERROR, "Init param '" + ANALYZER_FIELD_TYPE_PARAM + "' must be a <str>");
    }
    analyzerFieldType = analyzerFieldTypeParam.toString();

    if (0 < args.size()) {
      throw new SolrException(SERVER_ERROR, "Unexpected init param(s): '" + args.getName(0) + "'");
    }

    super.init(args);
  }

  /**
   * init helper method that should only be called when we know for certain that both the
   * "source" and "dest" init params do <em>not</em> exist.
   */
  @SuppressWarnings("unchecked")
  private void initSimpleRegexReplacement(@SuppressWarnings({"rawtypes"})NamedList args) {
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
  private void initSourceSelectorSyntax(@SuppressWarnings({"rawtypes"})NamedList args) {
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
        @SuppressWarnings({"rawtypes"})
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
          @SuppressWarnings({"rawtypes"})
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
      throw new SolrException(SERVER_ERROR,
          "Init params do not specify any field from which to extract entities, please supply either "
          + SOURCE_PARAM + " and " + DEST_PARAM + " or " + PATTERN_PARAM + " and " + REPLACEMENT_PARAM + ". See javadocs" +
          "for OpenNLPExtractNamedEntitiesUpdateProcessor for further details.");
    }

    if (d instanceof NamedList) {
      @SuppressWarnings({"rawtypes"})
      NamedList destList = (NamedList) d;

      Object patt = destList.remove(PATTERN_PARAM);
      Object replacement = destList.remove(REPLACEMENT_PARAM);

      if (null == patt || null == replacement) {
        throw new SolrException(SERVER_ERROR, "Init param '" + DEST_PARAM + "' children '" +
            PATTERN_PARAM + "' and '" + REPLACEMENT_PARAM +
            "' are both mandatory and can not be null");
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
    try {
      OpenNLPOpsFactory.getNERTaggerModel(modelFile, core.getResourceLoader());
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public final UpdateRequestProcessor getInstance
      (SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    final FieldNameSelector srcSelector = getSourceSelector();
    return new UpdateRequestProcessor(next) {
      private final NLPNERTaggerOp nerTaggerOp;
      private Analyzer analyzer = null;
      {
        try {
          nerTaggerOp = OpenNLPOpsFactory.getNERTagger(modelFile);
          FieldType fieldType = req.getSchema().getFieldTypeByName(analyzerFieldType);
          if (fieldType == null) {
            throw new SolrException
                (SERVER_ERROR, ANALYZER_FIELD_TYPE_PARAM + " '" + analyzerFieldType + "' not found in the schema.");
          }
          analyzer = fieldType.getIndexAnalyzer();
        } catch (IOException e) {
          throw new IllegalArgumentException(e);
        }
      }

      @Override
      public void processAdd(AddUpdateCommand cmd) throws IOException {

        final SolrInputDocument doc = cmd.getSolrInputDocument();

        // Destination may be regex replace string, or "{EntityType}" replaced by
        // each entity's type, both of which can cause multiple output fields.
        Map<String,SolrInputField> destMap = new HashMap<>();

        // preserve initial values
        for (final String fname : doc.getFieldNames()) {
          if ( ! srcSelector.shouldMutate(fname)) continue;

          Collection<Object> srcFieldValues = doc.getFieldValues(fname);
          if (srcFieldValues == null || srcFieldValues.isEmpty()) continue;

          String resolvedDest = dest;

          if (pattern != null) {
            Matcher matcher = pattern.matcher(fname);
            if (matcher.find()) {
              resolvedDest = matcher.replaceAll(dest);
            } else {
              log.debug("srcSelector.shouldMutate('{}') returned true, " +
                  "but replacement pattern did not match, field skipped.", fname);
              continue;
            }
          }

          for (Object val : srcFieldValues) {
            for (Pair<String,String> entity : extractTypedNamedEntities(val)) {
              SolrInputField destField = null;
              String entityName = entity.first();
              String entityType = entity.second();
              final String resolved = resolvedDest.replace(ENTITY_TYPE, entityType);
              if (doc.containsKey(resolved)) {
                destField = doc.getField(resolved);
              } else {
                SolrInputField targetField = destMap.get(resolved);
                if (targetField == null) {
                  destField = new SolrInputField(resolved);
                } else {
                  destField = targetField;
                }
              }
              destField.addValue(entityName);

              // put it in map to avoid concurrent modification...
              destMap.put(resolved, destField);
            }
          }
        }

        for (Map.Entry<String,SolrInputField> entry : destMap.entrySet()) {
          doc.put(entry.getKey(), entry.getValue());
        }
        super.processAdd(cmd);
      }

      /** Using configured NER model, extracts (name, type) pairs from the given source field value */
      private List<Pair<String,String>> extractTypedNamedEntities(Object srcFieldValue) throws IOException {
        List<Pair<String,String>> entitiesWithType = new ArrayList<>();
        List<String> terms = new ArrayList<>();
        List<Integer> startOffsets = new ArrayList<>();
        List<Integer> endOffsets = new ArrayList<>();
        String fullText = srcFieldValue.toString();
        TokenStream tokenStream = analyzer.tokenStream("", fullText);
        CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
        OffsetAttribute offsetAtt = tokenStream.addAttribute(OffsetAttribute.class);
        FlagsAttribute flagsAtt = tokenStream.addAttribute(FlagsAttribute.class);
        tokenStream.reset();
        synchronized (nerTaggerOp) {
          while (tokenStream.incrementToken()) {
            terms.add(termAtt.toString());
            startOffsets.add(offsetAtt.startOffset());
            endOffsets.add(offsetAtt.endOffset());
            boolean endOfSentence = 0 != (flagsAtt.getFlags() & OpenNLPTokenizer.EOS_FLAG_BIT);
            if (endOfSentence) {    // extract named entities one sentence at a time
              extractEntitiesFromSentence(fullText, terms, startOffsets, endOffsets, entitiesWithType);
            }
          }
          tokenStream.end();
          tokenStream.close();
          if (!terms.isEmpty()) { // In case last token of last sentence isn't properly flagged with EOS_FLAG_BIT
            extractEntitiesFromSentence(fullText, terms, startOffsets, endOffsets, entitiesWithType);
          }
          nerTaggerOp.reset();      // Forget all adaptive data collected during previous calls
        }
        return entitiesWithType;
      }

      private void extractEntitiesFromSentence(String fullText, List<String> terms, List<Integer> startOffsets,
                                               List<Integer> endOffsets, List<Pair<String,String>> entitiesWithType) {
        for (Span span : nerTaggerOp.getNames(terms.toArray(new String[terms.size()]))) {
          String text = fullText.substring(startOffsets.get(span.getStart()), endOffsets.get(span.getEnd() - 1));
          entitiesWithType.add(new Pair<>(text, span.getType()));
        }
        terms.clear();
        startOffsets.clear();
        endOffsets.clear();
      }
    };
  }

  /** macro */
  private static SelectorParams parseSelectorParams(@SuppressWarnings({"rawtypes"})NamedList args) {
    return FieldMutatingUpdateProcessorFactory.parseSelectorParams(args);
  }
}
