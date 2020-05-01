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
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 *   Identifies the language of a set of input fields.
 *   Also supports mapping of field names based on detected language.
 * </p>
 * See <a href="https://lucene.apache.org/solr/guide/7_4/detecting-languages-during-indexing.html">Detecting Languages During Indexing</a> in reference guide
 * @since 3.5
 * @lucene.experimental
 */
public abstract class LanguageIdentifierUpdateProcessor extends UpdateRequestProcessor implements LangIdParams {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected boolean enabled;

  protected String[] inputFields = {};
  protected String[] mapFields = {};
  protected Pattern mapPattern;
  protected String mapReplaceStr;
  protected String langField;
  protected String langsField; // MultiValued, contains all languages detected
  protected String docIdField;
  protected String fallbackValue;
  protected String[] fallbackFields = {};
  protected boolean enableMapping;
  protected boolean mapKeepOrig;
  protected boolean overwrite;
  protected boolean mapOverwrite;
  protected boolean mapIndividual;
  protected boolean enforceSchema;
  protected double threshold;
  protected HashSet<String> langWhitelist;
  protected HashSet<String> mapIndividualFieldsSet;
  protected HashSet<String> allMapFieldsSet;
  protected HashMap<String,String> lcMap;
  protected HashMap<String,String> mapLcMap;
  protected IndexSchema schema;
  protected int maxFieldValueChars;
  protected int maxTotalChars;

  // Regex patterns
  protected final Pattern tikaSimilarityPattern = Pattern.compile(".*\\((.*?)\\)");
  protected final Pattern langPattern = Pattern.compile("\\{lang\\}");

  public LanguageIdentifierUpdateProcessor(SolrQueryRequest req,
                                           SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(next);
    schema = req.getSchema();

    initParams(req.getParams());
  }

  private void initParams(SolrParams params) {
    if (params != null) {
      // Document-centric langId params
      setEnabled(params.getBool(LANGUAGE_ID, true));
      if(params.get(FIELDS_PARAM, "").length() > 0) {
        inputFields = params.get(FIELDS_PARAM, "").split(",");
      }
      langField = params.get(LANG_FIELD, DOCID_LANGFIELD_DEFAULT);
      langsField = params.get(LANGS_FIELD, DOCID_LANGSFIELD_DEFAULT);
      SchemaField uniqueKeyField = schema.getUniqueKeyField();
      docIdField = params.get(DOCID_PARAM, uniqueKeyField == null ? DOCID_FIELD_DEFAULT : uniqueKeyField.getName());
      fallbackValue = params.get(FALLBACK);
      if(params.get(FALLBACK_FIELDS, "").length() > 0) {
        fallbackFields = params.get(FALLBACK_FIELDS).split(",");
      }
      overwrite = params.getBool(OVERWRITE, false);
      langWhitelist = new HashSet<>();
      threshold = params.getDouble(THRESHOLD, DOCID_THRESHOLD_DEFAULT);
      if(params.get(LANG_WHITELIST, "").length() > 0) {
        for(String lang : params.get(LANG_WHITELIST, "").split(",")) {
          langWhitelist.add(lang);
        }
      }

      // Mapping params (field centric)
      enableMapping = params.getBool(MAP_ENABLE, false);
      if(params.get(MAP_FL, "").length() > 0) {
        mapFields = params.get(MAP_FL, "").split(",");
      } else {
        mapFields = inputFields;
      }
      mapKeepOrig = params.getBool(MAP_KEEP_ORIG, false);
      mapOverwrite = params.getBool(MAP_OVERWRITE, false);
      mapIndividual = params.getBool(MAP_INDIVIDUAL, false);

      // Process individual fields
      String[] mapIndividualFields = {};
      if(params.get(MAP_INDIVIDUAL_FL, "").length() > 0) {
        mapIndividualFields = params.get(MAP_INDIVIDUAL_FL, "").split(",");
      } else {
        mapIndividualFields = mapFields;
      }
      mapIndividualFieldsSet = new HashSet<>(Arrays.asList(mapIndividualFields));
      // Compile a union of the lists of fields to map
      allMapFieldsSet = new HashSet<>(Arrays.asList(mapFields));
      if(Arrays.equals(mapFields, mapIndividualFields)) {
        allMapFieldsSet.addAll(mapIndividualFieldsSet);
      }

      // Normalize detected langcode onto normalized langcode
      lcMap = new HashMap<>();
      if(params.get(LCMAP) != null) {
        for(String mapping : params.get(LCMAP).split("[, ]")) {
          String[] keyVal = mapping.split(":");
          if(keyVal.length == 2) {
            lcMap.put(keyVal[0], keyVal[1]);
          } else {
            log.error("Unsupported format for langid.lcmap: {}. Skipping this mapping.", mapping);
          }
        }
      }

      // Language Code mapping
      mapLcMap = new HashMap<>();
      if(params.get(MAP_LCMAP) != null) {
        for(String mapping : params.get(MAP_LCMAP).split("[, ]")) {
          String[] keyVal = mapping.split(":");
          if(keyVal.length == 2) {
            mapLcMap.put(keyVal[0], keyVal[1]);
          } else {
            log.error("Unsupported format for langid.map.lcmap: {}. Skipping this mapping.", mapping);
          }
        }
      }
      enforceSchema = params.getBool(ENFORCE_SCHEMA, true);

      mapPattern = Pattern.compile(params.get(MAP_PATTERN, MAP_PATTERN_DEFAULT));
      mapReplaceStr = params.get(MAP_REPLACE, MAP_REPLACE_DEFAULT);
      maxFieldValueChars = params.getInt(MAX_FIELD_VALUE_CHARS, MAX_FIELD_VALUE_CHARS_DEFAULT);
      maxTotalChars = params.getInt(MAX_TOTAL_CHARS, MAX_TOTAL_CHARS_DEFAULT);
      if (maxFieldValueChars > maxTotalChars) {
        if (maxTotalChars == MAX_TOTAL_CHARS_DEFAULT) {
          // If the user specified only maxFieldValueChars, make maxTotalChars the same as it
          log.warn("{} ({}) is less than {} ({}).  Setting {} to {}."
              , MAX_FIELD_VALUE_CHARS, maxFieldValueChars, MAX_TOTAL_CHARS
              , maxTotalChars, MAX_TOTAL_CHARS, maxFieldValueChars);
          maxTotalChars = maxFieldValueChars;
        } else {
          // If the user specified maxTotalChars, make maxFieldValueChars the same as it
          log.warn("{} ({}) is less than {} ({}).  Setting {} to {}."
              , MAX_FIELD_VALUE_CHARS, maxFieldValueChars, MAX_TOTAL_CHARS
              , maxTotalChars, MAX_FIELD_VALUE_CHARS, maxTotalChars );
          maxFieldValueChars = maxTotalChars;
        }
      }
    }
    log.debug("LangId configured");


    if (inputFields.length == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
              "Missing or faulty configuration of LanguageIdentifierUpdateProcessor. Input fields must be specified as a comma separated list");
    }

  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    if (isEnabled()) {
      process(cmd.getSolrInputDocument());
    } else {
      log.debug("Processor not enabled, not running");
    }
    super.processAdd(cmd);
  }

  /**
   * This is the main process method called from processAdd()
   * @param doc the SolrInputDocument to modify
   */
  protected void process(SolrInputDocument doc) {
    String docLang = null;
    HashSet<String> docLangs = new HashSet<>();
    String fallbackLang = getFallbackLang(doc, fallbackFields, fallbackValue);

    if(langField == null || !doc.containsKey(langField) || (doc.containsKey(langField) && overwrite)) {
      List<DetectedLanguage> languagelist = detectLanguage(doc);
      docLang = resolveLanguage(languagelist, fallbackLang);
      docLangs.add(docLang);
      if (log.isDebugEnabled()) {
        log.debug("Detected main document language from fields {}: {}", Arrays.toString(inputFields), docLang);
      }

      if(doc.containsKey(langField) && overwrite) {
        if (log.isDebugEnabled()) {
          log.debug("Overwritten old value {}", doc.getFieldValue(langField));
        }
      }
      if(langField != null && langField.length() != 0) {
        doc.setField(langField, docLang);
      }
    } else {
      // langField is set, we sanity check it against whitelist and fallback
      docLang = resolveLanguage(doc.getFieldValue(langField).toString(), fallbackLang);
      docLangs.add(docLang);
      log.debug("Field {} already contained value {}, not overwriting.", langField, docLang);
    }

    if(enableMapping) {
      for (String fieldName : allMapFieldsSet) {
        if(doc.containsKey(fieldName)) {
          String fieldLang;
          if(mapIndividual && mapIndividualFieldsSet.contains(fieldName)) {
            List<DetectedLanguage> languagelist = detectLanguage(solrDocReader(doc, new String[]{fieldName}));
            fieldLang = resolveLanguage(languagelist, docLang);
            docLangs.add(fieldLang);
            log.debug("Mapping field {} using individually detected language {}", fieldName, fieldLang);
          } else {
            fieldLang = docLang;
            log.debug("Mapping field {} using document global language {}", fieldName, fieldLang);
          }
          String mappedOutputField = getMappedField(fieldName, fieldLang);

          if (mappedOutputField != null) {
            if (log.isDebugEnabled()) {
              log.debug("Mapping field {} to {}", doc.getFieldValue(docIdField), fieldLang);
            }
            SolrInputField inField = doc.getField(fieldName);
            doc.setField(mappedOutputField, inField.getValue());
            if(!mapKeepOrig) {
              log.debug("Removing old field {}", fieldName);
              doc.removeField(fieldName);
            }
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid output field mapping for "
                    + fieldName + " field and language: " + fieldLang);
          }
        }
      }
    }

    // Set the languages field to an array of all detected languages
    if(langsField != null && langsField.length() != 0) {
      doc.setField(langsField, docLangs.toArray());
    }
  }

  /**
   * Decides the fallback language, either from content of fallback field or fallback value
   * @param doc the Solr document
   * @param fallbackFields an array of strings with field names containing fallback language codes
   * @param fallbackValue a language code to use in case no fallbackFields are found
   */
  private String getFallbackLang(SolrInputDocument doc, String[] fallbackFields, String fallbackValue) {
    String lang = null;
    for(String field : fallbackFields) {
      if(doc.containsKey(field)) {
        lang = (String) doc.getFieldValue(field);
        log.debug("Language fallback to field {}", field);
        break;
      }
    }
    if(lang == null) {
      log.debug("Language fallback to value {}", fallbackValue);
      lang = fallbackValue;
    }
    return lang;
  }

  /**
   * Detects language(s) from all configured fields
   * @param doc The solr document
   * @return List of detected language(s) according to RFC-3066
   */
  protected List<DetectedLanguage> detectLanguage(SolrInputDocument doc) {
    return detectLanguage(solrDocReader(doc, inputFields));
  }

  /**
   * Detects language(s) from a reader, typically based on some fields in SolrInputDocument
   * Classes wishing to implement their own language detection module should override this method.
   * @param solrDocReader A reader serving the text from the document to detect
   * @return List of detected language(s) according to RFC-3066
   */
  protected abstract List<DetectedLanguage> detectLanguage(Reader solrDocReader);

  /**
   * Chooses a language based on the list of candidates detected
   * @param language language code as a string
   * @param fallbackLang the language code to use as a fallback
   * @return a string of the chosen language
   */
  protected String resolveLanguage(String language, String fallbackLang) {
    List<DetectedLanguage> l = new ArrayList<>();
    l.add(new DetectedLanguage(language, 1.0));
    return resolveLanguage(l, fallbackLang);
  }

  /**
   * Chooses a language based on the list of candidates detected
   * @param languages a List of DetectedLanguages with certainty score
   * @param fallbackLang the language code to use as a fallback
   * @return a string of the chosen language
   */
  protected String resolveLanguage(List<DetectedLanguage> languages, String fallbackLang) {
    String langStr;
    if(languages.size() == 0) {
      log.debug("No language detected, using fallback {}", fallbackLang);
      langStr = fallbackLang;
    } else {
      DetectedLanguage lang = languages.get(0);
      String normalizedLang = normalizeLangCode(lang.getLangCode());
      if(langWhitelist.isEmpty() || langWhitelist.contains(normalizedLang)) {
        if (log.isDebugEnabled()) {
          log.debug("Language detected {} with certainty {}", normalizedLang, lang.getCertainty());
        }
        if(lang.getCertainty() >= threshold) {
          langStr = normalizedLang;
        } else {
          log.debug("Detected language below threshold {}, using fallback {}", threshold, fallbackLang);
          langStr = fallbackLang;
        }
      } else {
        if (log.isDebugEnabled()) {
          log.debug("Detected a language not in whitelist ({}), using fallback {}", lang.getLangCode(), fallbackLang);
        }
        langStr = fallbackLang;
      }
    }

    if(langStr == null || langStr.length() == 0) {
      log.warn("Language resolved to null or empty string. Fallback not configured?");
      langStr = "";
    }

    return langStr;
  }

  /**
   * Looks up language code in map (langid.lcmap) and returns mapped value
   * @param langCode the language code string returned from detector
   * @return the normalized/mapped language code
   */
  protected String normalizeLangCode(String langCode) {
    if (lcMap.containsKey(langCode)) {
      String lc = lcMap.get(langCode);
      log.debug("Doing langcode normalization mapping from {} to {}", langCode, lc);
      return lc;
    }
    return langCode;
  }

  /**
   * Returns the name of the field to map the current contents into, so that they are properly analyzed.  For instance
   * if the currentField is "text" and the code is "en", the new field would by default be "text_en".
   * This method also performs custom regex pattern replace if configured. If enforceSchema=true
   * and the resulting field name doesn't exist, then null is returned.
   *
   * @param currentField The current field name
   * @param language the language code
   * @return The new schema field name, based on pattern and replace, or null if illegal
   */
  protected String getMappedField(String currentField, String language) {
    String lc = mapLcMap.containsKey(language) ? mapLcMap.get(language) : language;
    String newFieldName = langPattern.matcher(mapPattern.matcher(currentField).replaceFirst(mapReplaceStr)).replaceFirst(lc);
    if(enforceSchema && schema.getFieldOrNull(newFieldName) == null) {
      log.warn("Unsuccessful field name mapping from {} to {}, field does not exist and enforceSchema=true; skipping mapping.", currentField, newFieldName);
      return null;
    } else {
      log.debug("Doing mapping from {} with language {} to field {}", currentField, language, newFieldName);
    }
    return newFieldName;
  }

  /**
   * Tells if this processor is enabled or not
   * @return true if enabled, else false
   */
  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * Returns a reader that streams String content from fields.
   * This is more memory efficient than building a full string buffer
   * @param doc the solr document
   * @param fields the field names to read
   * @return a reader over the fields
   */
  protected SolrInputDocumentReader solrDocReader(SolrInputDocument doc, String[] fields) {
    return new SolrInputDocumentReader(doc, fields, maxTotalChars, maxFieldValueChars, " ");
  }
  
  /**
   * Concatenates content from input fields defined in langid.fl.
   * For test purposes only
   */
  protected String concatFields(SolrInputDocument doc) {
    return SolrInputDocumentReader.asString(solrDocReader(doc, inputFields));
  }
}
