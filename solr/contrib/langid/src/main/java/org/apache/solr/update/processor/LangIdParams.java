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

public interface LangIdParams {

  String LANGUAGE_ID = "langid";
  String DOCID_PARAM =  LANGUAGE_ID + ".idField";

  String FIELDS_PARAM = LANGUAGE_ID + ".fl";                 // Field list to detect from
  String LANG_FIELD = LANGUAGE_ID + ".langField";            // Main language detected
  String LANGS_FIELD = LANGUAGE_ID + ".langsField";          // All languages detected (multiValued)
  String FALLBACK =  LANGUAGE_ID + ".fallback";              // Fallback lang code  
  String FALLBACK_FIELDS =  LANGUAGE_ID + ".fallbackFields"; // Comma-sep list of fallback fields
  String OVERWRITE  = LANGUAGE_ID + ".overwrite";            // Overwrite if existing language value in LANG_FIELD
  String THRESHOLD  = LANGUAGE_ID + ".threshold";            // Detection threshold
  String ENFORCE_SCHEMA =  LANGUAGE_ID + ".enforceSchema";   // Enforces that output fields exist in schema
  String LANG_WHITELIST  = LANGUAGE_ID + ".whitelist";       // Allowed languages
  String LCMAP =  LANGUAGE_ID + ".lcmap";                    // Maps detected langcode to other value
  String MAP_ENABLE =  LANGUAGE_ID + ".map";                 // Turns on or off the field mapping
  String MAP_FL =  LANGUAGE_ID + ".map.fl";                  // Field list for mapping
  String MAP_OVERWRITE =  LANGUAGE_ID + ".map.overwrite";    // Whether to overwrite existing fields
  String MAP_KEEP_ORIG =  LANGUAGE_ID + ".map.keepOrig";     // Keep original field after mapping
  String MAP_INDIVIDUAL =  LANGUAGE_ID + ".map.individual";  // Detect language per individual field
  String MAP_INDIVIDUAL_FL =  LANGUAGE_ID + ".map.individual.fl";// Field list of fields to redetect language for
  String MAP_LCMAP =  LANGUAGE_ID + ".map.lcmap";            // Enables mapping multiple langs to same output field
  String MAP_PATTERN =  LANGUAGE_ID + ".map.pattern";        // RegEx pattern to match field name
  String MAP_REPLACE =  LANGUAGE_ID + ".map.replace";        // Replace pattern
  String MAX_FIELD_VALUE_CHARS = LANGUAGE_ID + ".maxFieldValueChars";   // Maximum number of characters to use per field for language detection
  String MAX_TOTAL_CHARS = LANGUAGE_ID + ".maxTotalChars";   // Maximum number of characters to use per all concatenated fields for language detection

  String DOCID_FIELD_DEFAULT = "id";
  String DOCID_LANGFIELD_DEFAULT = null;
  String DOCID_LANGSFIELD_DEFAULT = null;
  String MAP_PATTERN_DEFAULT = "(.*)";
  String MAP_REPLACE_DEFAULT = "$1_{lang}";
  int MAX_FIELD_VALUE_CHARS_DEFAULT = 10000;
  int MAX_TOTAL_CHARS_DEFAULT = 20000;

  // TODO: This default threshold accepts even "uncertain" detections. 
  // Increase &langid.threshold above 0.5 to return only certain detections
  Double DOCID_THRESHOLD_DEFAULT = 0.5;
}
