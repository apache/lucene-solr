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

package org.apache.lucene.luke.app.controllers.dto.documents;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;

import java.math.BigDecimal;
import java.math.BigInteger;

public class DocumentField {

  private String field;
  private String flag;
  private Long norm;
  private String value;

  public static DocumentField of(org.apache.lucene.luke.models.documents.DocumentField f) {
    DocumentField dField = new DocumentField();
    dField.field = f.getName();
    dField.flag = flags(f);
    dField.norm = f.getNorm();
    if (f.getStringValue() != null) {
      dField.value = f.getStringValue();
    } else if (f.getNumericValue() != null) {
      dField.value = String.valueOf(f.getNumericValue());
    } else if (f.getBinaryValue() != null) {
      dField.value = String.valueOf(f.getBinaryValue());
    }
    return dField;
  }

  private DocumentField() {
  }

  public String getField() {
    return field;
  }

  public String getFlag() {
    return flag;
  }

  public Long getNorm() {
    return norm;
  }

  public String getValue() {
    return value;
  }

  private static String flags(org.apache.lucene.luke.models.documents.DocumentField f) {
    StringBuilder sb = new StringBuilder();
    // index options
    if (f.getIdxOptions() == null || f.getIdxOptions() == IndexOptions.NONE) {
      sb.append("-----");
    } else {
      sb.append("I");
      switch (f.getIdxOptions()) {
        case DOCS:
          sb.append("d---");
          break;
        case DOCS_AND_FREQS:
          sb.append("df--");
          break;
        case DOCS_AND_FREQS_AND_POSITIONS:
          sb.append("dfp-");
          break;
        case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
          sb.append("dfpo");
          break;
        default:
          sb.append("----");
      }
    }
    // has norm?
    if (f.hasNorms()) {
      sb.append("N");
    } else {
      sb.append("-");
    }
    // has payloads?
    if (f.hasPayloads()) {
      sb.append("P");
    } else {
      sb.append("-");
    }
    // stored?
    if (f.isStored()) {
      sb.append("S");
    } else {
      sb.append("-");
    }
    // binary?
    if (f.getBinaryValue() != null) {
      sb.append("B");
    } else {
      sb.append("-");
    }
    // numeric?
    if (f.getNumericValue() == null) {
      sb.append("----");
    } else {
      sb.append("#");
      // try faking it
      Number numeric = f.getNumericValue();
      if (numeric instanceof Integer) {
        sb.append("i32");
      } else if (numeric instanceof Long) {
        sb.append("i64");
      } else if (numeric instanceof Float) {
        sb.append("f32");
      } else if (numeric instanceof Double) {
        sb.append("f64");
      } else if (numeric instanceof Short) {
        sb.append("i16");
      } else if (numeric instanceof Byte) {
        sb.append("i08");
      } else if (numeric instanceof BigDecimal) {
        sb.append("b^d");
      } else if (numeric instanceof BigInteger) {
        sb.append("b^i");
      } else {
        sb.append("???");
      }
    }
    // has term vector?
    if (f.hasTermVectors()) {
      sb.append("V");
    } else {
      sb.append("-");
    }
    // doc values
    if (f.getDvType() == null || f.getDvType() == DocValuesType.NONE) {
      sb.append("-------");
    } else {
      sb.append("D");
      switch (f.getDvType()) {
        case NUMERIC:
          sb.append("number");
          break;
        case BINARY:
          sb.append("binary");
          break;
        case SORTED:
          sb.append("sorted");
          break;
        case SORTED_NUMERIC:
          sb.append("srtnum");
          break;
        case SORTED_SET:
          sb.append("srtset");
          break;
        default:
          sb.append("??????");
      }
    }
    // point values
    if (f.getPointDimensionCount() == 0) {
      sb.append("----");
    } else {
      sb.append("T");
      sb.append(f.getPointNumBytes());
      sb.append("/");
      sb.append(f.getPointDimensionCount());
    }
    return sb.toString();
  }

}
