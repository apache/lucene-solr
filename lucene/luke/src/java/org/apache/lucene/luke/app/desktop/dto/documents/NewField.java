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

package org.apache.lucene.luke.app.desktop.dto.documents;

import java.util.Objects;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.luke.app.desktop.util.NumericUtils;

/** Data holder for a new field. This is used in the add document dialog. */
public final class NewField {

  private boolean deleted;

  private String name;

  private Class<? extends IndexableField> type;

  private String value;

  private IndexableFieldType fieldType;

  private boolean stored;

  public static NewField newInstance() {
    NewField f = new NewField();
    f.deleted = false;
    f.name = "";
    f.type = TextField.class;
    f.value = "";
    f.fieldType = new TextField("", "", Field.Store.NO).fieldType();
    f.stored = f.fieldType.stored();
    return f;
  }

  private NewField() {}

  public boolean isDeleted() {
    return deleted;
  }

  public boolean deletedProperty() {
    return deleted;
  }

  public void setDeleted(boolean value) {
    deleted = value;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = Objects.requireNonNull(name);
  }

  public Class<? extends IndexableField> getTypeProperty() {
    return type;
  }

  public Class<? extends IndexableField> getType() {
    return type;
  }

  public void setType(Class<? extends IndexableField> type) {
    this.type = Objects.requireNonNull(type);
  }

  public void resetFieldType(Class<?> type) {
    if (type.equals(TextField.class)) {
      fieldType = new TextField("", "", Field.Store.NO).fieldType();
    } else if (type.equals(StringField.class)) {
      fieldType = new StringField("", "", Field.Store.NO).fieldType();
    } else if (type.equals(IntPoint.class)) {
      fieldType = new IntPoint("", NumericUtils.convertToIntArray(value, true)).fieldType();
    } else if (type.equals(LongPoint.class)) {
      fieldType = new LongPoint("", NumericUtils.convertToLongArray(value, true)).fieldType();
    } else if (type.equals(FloatPoint.class)) {
      fieldType = new FloatPoint("", NumericUtils.convertToFloatArray(value, true)).fieldType();
    } else if (type.equals(DoublePoint.class)) {
      fieldType = new DoublePoint("", NumericUtils.convertToDoubleArray(value, true)).fieldType();
    } else if (type.equals(SortedDocValuesField.class)) {
      fieldType = new SortedDocValuesField("", null).fieldType();
    } else if (type.equals(SortedSetDocValuesField.class)) {
      fieldType = new SortedSetDocValuesField("", null).fieldType();
    } else if (type.equals(NumericDocValuesField.class)) {
      fieldType = new NumericDocValuesField("", 0).fieldType();
    } else if (type.equals(SortedNumericDocValuesField.class)) {
      fieldType = new SortedNumericDocValuesField("", 0).fieldType();
    } else if (type.equals(StoredField.class)) {
      fieldType = new StoredField("", "").fieldType();
    } else if (type.equals(Field.class)) {
      fieldType = new FieldType(this.fieldType);
    }
  }

  public IndexableFieldType getFieldType() {
    return fieldType;
  }

  public boolean isStored() {
    return stored;
  }

  public void setStored(boolean stored) {
    this.stored = stored;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = Objects.requireNonNull(value);
  }
}
