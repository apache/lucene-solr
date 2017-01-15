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
package org.apache.solr.client.solrj.response;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.luke.FieldFlag;
import org.apache.solr.common.util.NamedList;


/**
 * This is an incomplete representation of the data returned from Luke
 *
 *
 * @since solr 1.3
 */
public class LukeResponse extends SolrResponseBase {

  public static class FieldTypeInfo implements Serializable {
    String name;
    String className;
    boolean tokenized;
    String analyzer;
    List<String> fields;
    List<String> dynamicFields;


    public FieldTypeInfo(String name) {
      this.name = name;
      fields = Collections.emptyList();
    }


    public String getAnalyzer() {
      return analyzer;
    }

    public String getClassName() {
      return className;
    }

    public List<String> getFields() {
      return fields;
    }

    public List<String> getDynamicFields() {
      return dynamicFields;
    }

    public String getName() {
      return name;
    }

    public boolean isTokenized() {
      return tokenized;
    }/*
     Sample:
     types={ignored={fields=null,tokenized=false,analyzer=org.apache.solr.schema.FieldType$DefaultAnalyzer@f94934},
     integer={fields=null,tokenized=false,analyzer=org.apache.solr.schema.FieldType$DefaultAnalyzer@3525a2},
     sfloat={fields=[price, weight],tokenized=false,analyzer=org.apache.solr.schema.FieldType$DefaultAnalyzer@39cf9c},
     text_ws={fields=[cat],tokenized=true,analyzer=TokenizerChain(org.apache.solr.analysis.WhitespaceTokenizerFactory@6d3ca2)},
     alphaOnlySort={fields=[alphaNameSort],tokenized=true,analyzer=TokenizerChain(org.apache.solr.analysis.KeywordTokenizerFactory@a7bd3b,
      org.apache.solr.analysis.LowerCaseFilterFactory@78aae2, org.apache.solr.analysis.TrimFilterFactory@1b16a7,
      org.apache.solr.analysis.PatternReplaceFilterFactory@6c6b08)},date={fields=[timestamp],tokenized=false,
      analyzer=org.apache.solr.schema.FieldType$DefaultAnalyzer@e6e42e},sint={fields=[popularity],
      tokenized=false,analyzer=org.apache.solr.schema.FieldType$DefaultAnalyzer@8ea21d},
      boolean={fields=[inStock],tokenized=false,analyzer=org.apache.solr.schema.BoolField$1@354949},
      textTight={fields=[sku],tokenized=true,analyzer=TokenizerChain(org.apache.solr.analysis.WhitespaceTokenizerFactory@5e88f7,
       org.apache.solr.analysis.SynonymFilterFactory@723646, org.apache.solr.analysis.StopFilterFactory@492ff1,
       org.apache.solr.analysis.WordDelimiterFilterFactory@eaabad, org.apache.solr.analysis.LowerCaseFilterFactory@ad1355,
        org.apache.solr.analysis.EnglishPorterFilterFactory@d03a00, org.apache.solr.analysis.RemoveDuplicatesTokenFilterFactory@900079)},
        long={fields=null,tokenized=false,analyzer=org.apache.solr.schema.FieldType$DefaultAnalyzer@f3b83},
        double={fields=null,tokenized=false,analyzer=org.apache.solr.schema.FieldType$DefaultAnalyzer@c2b07},

      */

    @SuppressWarnings("unchecked")
    public void read(NamedList<Object> nl) {
      for (Map.Entry<String, Object> entry : nl) {
        String key = entry.getKey();
        if ("fields".equals(key) && entry.getValue() != null) {
          List<String> theFields = (List<String>) entry.getValue();
          fields = new ArrayList<>(theFields);
        } else if ("dynamicFields".equals(key) && entry.getValue() != null) {
          List<String> theDynamicFields = (List<String>) entry.getValue();
          dynamicFields = new ArrayList<>(theDynamicFields);
        } else if ("tokenized".equals(key) == true) {
          tokenized = Boolean.parseBoolean(entry.getValue().toString());
        } else if ("analyzer".equals(key) == true) {
          analyzer = entry.getValue().toString();
        } else if ("className".equals(key) == true) {
          className = entry.getValue().toString();
        }
      }
    }
  }

  public static class FieldInfo implements Serializable {
    String name;
    String type;
    String schema;
    int docs;
    int distinct;
    EnumSet<FieldFlag> flags;
    boolean cacheableFaceting;
    NamedList<Integer> topTerms;

    public FieldInfo(String n) {
      name = n;
    }

    @SuppressWarnings("unchecked")
    public void read(NamedList<Object> nl) {
      for (Map.Entry<String, Object> entry : nl) {
        if ("type".equals(entry.getKey())) {
          type = (String) entry.getValue();
        }
        if ("flags".equals(entry.getKey())) {
          flags = parseFlags((String) entry.getValue());
        } else if ("schema".equals(entry.getKey())) {
          schema = (String) entry.getValue();
        } else if ("docs".equals(entry.getKey())) {
          docs = (Integer) entry.getValue();
        } else if ("distinct".equals(entry.getKey())) {
          distinct = (Integer) entry.getValue();
        } else if ("cacheableFaceting".equals(entry.getKey())) {
          cacheableFaceting = (Boolean) entry.getValue();
        } else if ("topTerms".equals(entry.getKey())) {
          topTerms = (NamedList<Integer>) entry.getValue();
        }
      }
    }

    public static EnumSet<FieldFlag> parseFlags(String flagStr) {
      EnumSet<FieldFlag> result = EnumSet.noneOf(FieldFlag.class);
      char[] chars = flagStr.toCharArray();
      for (int i = 0; i < chars.length; i++) {
        if (chars[i] != '-') {
          FieldFlag flag = FieldFlag.getFlag(chars[i]);
          result.add(flag);
        }
      }
      return result;
    }

    public EnumSet<FieldFlag> getFlags() {
      return flags;
    }

    public boolean isCacheableFaceting() {
      return cacheableFaceting;
    }

    public String getType() {
      return type;
    }

    public int getDistinct() {
      return distinct;
    }

    public int getDocs() {
      return docs;
    }

    public String getName() {
      return name;
    }

    public String getSchema() {
      return schema;
    }

    public EnumSet<FieldFlag> getSchemaFlags() {
      return flags;
    }

    public NamedList<Integer> getTopTerms() {
      return topTerms;
    }
  }

  private NamedList<Object> indexInfo;
  private Map<String, FieldInfo> fieldInfo;
  private Map<String, FieldInfo> dynamicFieldInfo;
  private Map<String, FieldTypeInfo> fieldTypeInfo;

  @Override
  @SuppressWarnings("unchecked")
  public void setResponse(NamedList<Object> res) {
    super.setResponse(res);

    // Parse indexinfo
    indexInfo = (NamedList<Object>) res.get("index");

    NamedList<Object> schema = (NamedList<Object>) res.get("schema");
    NamedList<Object> flds = (NamedList<Object>) res.get("fields");
    NamedList<Object> dynamicFlds = (NamedList<Object>) res.get("dynamicFields");

    if (flds == null && schema != null ) {
      flds = (NamedList<Object>) schema.get("fields");
    }
    if (flds != null) {
      fieldInfo = new HashMap<>();
      for (Map.Entry<String, Object> field : flds) {
        FieldInfo f = new FieldInfo(field.getKey());
        f.read((NamedList<Object>) field.getValue());
        fieldInfo.put(field.getKey(), f);
      }
    }

    if (dynamicFlds == null && schema != null) {
      dynamicFlds = (NamedList<Object>) schema.get("dynamicFields");
    }
    if (dynamicFlds != null) {
      dynamicFieldInfo = new HashMap<>();
      for (Map.Entry<String, Object> dynamicField : dynamicFlds) {
        FieldInfo f = new FieldInfo(dynamicField.getKey());
        f.read((NamedList<Object>) dynamicField.getValue());
        dynamicFieldInfo.put(dynamicField.getKey(), f);
      }
    }

    if( schema != null ) {
      NamedList<Object> fldTypes = (NamedList<Object>) schema.get("types");
      if (fldTypes != null) {
        fieldTypeInfo = new HashMap<>();
        for (Map.Entry<String, Object> fieldType : fldTypes) {
          FieldTypeInfo ft = new FieldTypeInfo(fieldType.getKey());
          ft.read((NamedList<Object>) fieldType.getValue());
          fieldTypeInfo.put(fieldType.getKey(), ft);
        }
      }
    }
  }

  //----------------------------------------------------------------
  //----------------------------------------------------------------

  public String getIndexDirectory() {
    if (indexInfo == null) return null;
    return (String) indexInfo.get("directory");
  }

  public Integer getNumDocs() {
    if (indexInfo == null) return null;
    return (Integer) indexInfo.get("numDocs");
  }

  public Integer getMaxDoc() {
    if (indexInfo == null) return null;
    return (Integer) indexInfo.get("maxDoc");
  }

  public Integer getNumTerms() {
    if (indexInfo == null) return null;
    return (Integer) indexInfo.get("numTerms");
  }

  public Map<String, FieldTypeInfo> getFieldTypeInfo() {
    return fieldTypeInfo;
  }

  public FieldTypeInfo getFieldTypeInfo(String name) {
    return fieldTypeInfo.get(name);
  }

  public NamedList<Object> getIndexInfo() {
    return indexInfo;
  }

  public Map<String, FieldInfo> getFieldInfo() {
    return fieldInfo;
  }

  public FieldInfo getFieldInfo(String f) {
    return fieldInfo.get(f);
  }

  public Map<String, FieldInfo> getDynamicFieldInfo() {
    return dynamicFieldInfo;
  }

  public FieldInfo getDynamicFieldInfo(String f) {
    return dynamicFieldInfo.get(f);
  }

  //----------------------------------------------------------------
}
