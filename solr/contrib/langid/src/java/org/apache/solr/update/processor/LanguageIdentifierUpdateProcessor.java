package org.apache.solr.update.processor;

/**
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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.tika.language.LanguageIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;


/**
 * Identifies the language of a set of input fields using Tika's
 * LanguageIdentifier. Also supports mapping of field names based
 * on detected language. 
 * The tika-core-x.y.jar must be on the classpath
 * <p>
 * See <a href="http://wiki.apache.org/solr/LanguageDetection">http://wiki.apache.org/solr/LanguageDetection</a>
 * @since 3.5
 */
public class LanguageIdentifierUpdateProcessor extends UpdateRequestProcessor implements LangIdParams {

  protected final static Logger log = LoggerFactory
          .getLogger(LanguageIdentifierUpdateProcessor.class);

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
  protected IndexSchema schema;
  
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
      docIdField = params.get(DOCID_PARAM, DOCID_FIELD_DEFAULT);
      fallbackValue = params.get(FALLBACK);
      if(params.get(FALLBACK_FIELDS, "").length() > 0) {
        fallbackFields = params.get(FALLBACK_FIELDS).split(",");
      }
      overwrite = params.getBool(OVERWRITE, false);
      langWhitelist = new HashSet<String>();
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
      mapIndividualFieldsSet = new HashSet<String>(Arrays.asList(mapIndividualFields));
      // Compile a union of the lists of fields to map
      allMapFieldsSet = new HashSet<String>(Arrays.asList(mapFields));
      if(Arrays.equals(mapFields, mapIndividualFields)) {
        allMapFieldsSet.addAll(mapIndividualFieldsSet);
      }
      
      // Language Code mapping
      lcMap = new HashMap<String,String>();
      if(params.get(MAP_LCMAP) != null) {
        for(String mapping : params.get(MAP_LCMAP).split("[, ]")) {
          String[] keyVal = mapping.split(":");
          if(keyVal.length == 2) {
            lcMap.put(keyVal[0], keyVal[1]);
          } else {
            log.error("Unsupported format for langid.map.lcmap: "+mapping+". Skipping this mapping.");
          }
        }
      }
      enforceSchema = params.getBool(ENFORCE_SCHEMA, true);

      mapPattern = Pattern.compile(params.get(MAP_PATTERN, MAP_PATTERN_DEFAULT));
      mapReplaceStr = params.get(MAP_REPLACE, MAP_REPLACE_DEFAULT);
      
      
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
   * This is the main, testable process method called from processAdd()
   * @param doc the SolrInputDocument to work on
   * @return the modified SolrInputDocument
   */
  protected SolrInputDocument process(SolrInputDocument doc) {
    String docLang = null;
    HashSet<String> docLangs = new HashSet<String>();
    String fallbackLang = getFallbackLang(doc, fallbackFields, fallbackValue);
      
    if(langField == null || !doc.containsKey(langField) || (doc.containsKey(langField) && overwrite)) {
      String allText = concatFields(doc, inputFields);
      List<DetectedLanguage> languagelist = detectLanguage(allText);
      docLang = resolveLanguage(languagelist, fallbackLang);
      docLangs.add(docLang);
      log.debug("Detected main document language from fields "+inputFields+": "+docLang);

      if(doc.containsKey(langField) && overwrite) {
        log.debug("Overwritten old value "+doc.getFieldValue(langField));
      }
      if(langField != null && langField.length() != 0) {
        doc.setField(langField, docLang);
      }
    } else {
      // langField is set, we sanity check it against whitelist and fallback
      docLang = resolveLanguage((String) doc.getFieldValue(langField), fallbackLang);
      docLangs.add(docLang);
      log.debug("Field "+langField+" already contained value "+docLang+", not overwriting.");
    }

    if(enableMapping) {
      for (String fieldName : allMapFieldsSet) {
        if(doc.containsKey(fieldName)) {        
          String fieldLang;
          if(mapIndividual && mapIndividualFieldsSet.contains(fieldName)) {
            String text = (String) doc.getFieldValue(fieldName);
            List<DetectedLanguage> languagelist = detectLanguage(text);
            fieldLang = resolveLanguage(languagelist, docLang);
            docLangs.add(fieldLang);
            log.debug("Mapping field "+fieldName+" using individually detected language "+fieldLang);
          } else {
            fieldLang = docLang;
            log.debug("Mapping field "+fieldName+" using document global language "+fieldLang);
          }
          String mappedOutputField = getMappedField(fieldName, fieldLang);
          if(enforceSchema && schema.getFieldOrNull(fieldName) == null) {
            log.warn("Unsuccessful field name mapping to {}, field does not exist, skipping mapping.", mappedOutputField, fieldName);
            mappedOutputField = fieldName;
          }
            
          if (mappedOutputField != null) {
            log.debug("Mapping field {} to {}", doc.getFieldValue(docIdField), fieldLang);
            SolrInputField inField = doc.getField(fieldName);
            doc.setField(mappedOutputField, inField.getValue(), inField.getBoost());
            if(!mapKeepOrig) {
              log.debug("Removing old field {}", fieldName);
              doc.removeField(fieldName);
            }
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid output field mapping for "
                    + fieldName + " field and language: " + fieldLang);
          }
        } else {
          log.warn("Document {} does not contain input field {}. Skipping this field.", doc.getFieldValue(docIdField), fieldName);
        }
      }
    }
    
    // Set the languages field to an array of all detected languages
    if(langsField != null && langsField.length() != 0) {
      doc.setField(langsField, docLangs.toArray());
    }
    
    return doc;
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
        log.debug("Language fallback to field "+field);
        break;
      }
    }
    if(lang == null) {
      log.debug("Language fallback to value "+fallbackValue);
      lang = fallbackValue;
    }
    return lang;
  }

  /*
   * Concatenates content from multiple fields
   */
  protected String concatFields(SolrInputDocument doc, String[] fields) {
    StringBuffer sb = new StringBuffer();
    for (String fieldName : inputFields) {
      log.debug("Appending field "+fieldName);
      if (doc.containsKey(fieldName)) {
        Object content = doc.getFieldValue(fieldName);
        if(content instanceof String) {
          sb.append((String) doc.getFieldValue(fieldName));
          sb.append(" ");
        } else {
          log.warn("Field "+fieldName+" not a String value, not including in detection");          
        }
      }
    }
    return sb.toString();
  }
  
  /**
   * Detects language(s) from a string.
   * Classes wishing to implement their own language detection module should override this method.
   * @param content The content to identify
   * @return List of detected language(s) according to RFC-3066
   */
  protected List<DetectedLanguage> detectLanguage(String content) {
    List<DetectedLanguage> languages = new ArrayList<DetectedLanguage>();
    if(content.trim().length() != 0) { 
      LanguageIdentifier identifier = new LanguageIdentifier(content.toString());
      // FIXME: Hack - we get the distance from toString and calculate our own certainty score
      Double distance = Double.parseDouble(tikaSimilarityPattern.matcher(identifier.toString()).replaceFirst("$1"));
      // This formula gives: 0.02 => 0.8, 0.1 => 0.5 which is a better sweetspot than isReasonablyCertain()
      Double certainty = 1 - (5 * distance); 
      certainty = (certainty < 0) ? 0 : certainty;
      DetectedLanguage language = new DetectedLanguage(identifier.getLanguage(), certainty);
      languages.add(language);
      log.debug("Language detected as "+language+" with a certainty of "+language.getCertainty()+" (Tika distance="+identifier.toString()+")");
    } else {
      log.debug("No input text to detect language from, returning empty list");
    }
    return languages;
  }

  /**
   * Chooses a language based on the list of candidates detected 
   * @param language language code as a string
   * @param fallbackLang the language code to use as a fallback
   * @return a string of the chosen language
   */
  protected String resolveLanguage(String language, String fallbackLang) {
    List<DetectedLanguage> l = new ArrayList<DetectedLanguage>();
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
      if(langWhitelist.isEmpty() || langWhitelist.contains(lang.getLangCode())) {
        log.debug("Language detected {} with certainty {}", lang.getLangCode(), lang.getCertainty());
        if(lang.getCertainty() >= threshold) {
          langStr = lang.getLangCode();
        } else {
          log.debug("Detected language below threshold {}, using fallback {}", threshold, fallbackLang);
          langStr = fallbackLang;
        }
      } else {
        log.debug("Detected a language not in whitelist ({}), using fallback {}", lang.getLangCode(), fallbackLang);
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
   * Returns the name of the field to map the current contents into, so that they are properly analyzed.  For instance
   * if the currentField is "text" and the code is "en", the new field would be "text_en".  If such a field doesn't exist,
   * then null is returned.
   *
   * @param currentField The current field name
   * @param language the language code
   * @return The new schema field name, based on pattern and replace
   */
  protected String getMappedField(String currentField, String language) {
    String lc = lcMap.containsKey(language) ? lcMap.get(language) : language;
    String newFieldName = langPattern.matcher(mapPattern.matcher(currentField).replaceFirst(mapReplaceStr)).replaceFirst(lc);
    log.debug("Doing mapping from "+currentField+" with language "+language+" to field "+newFieldName);
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

}
