package org.apache.lucene.server;

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

import java.util.Map;

import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;

/** Implements {@link Bindings} on top of the registered
 *  fields. */
public final class FieldDefBindings extends Bindings {

  private final Map<String,FieldDef> fields;

  /** Sole constructor. */
  public FieldDefBindings(Map<String,FieldDef> fields) {
    this.fields = fields;
  }

  @Override
  public ValueSource getValueSource(String name) {
    if (name.equals("_score")) {
      return getScoreValueSource();
    }
    FieldDef fd = fields.get(name);
    if (fd == null) {
      throw new IllegalArgumentException("Invalid reference '" + name + "'");
    }
    if (fd.valueType.equals("virtual")) {
      return fd.valueSource;
    } else if (fd.fieldType != null && fd.fieldType.docValueType() == DocValuesType.NUMERIC) {
      if (fd.valueType.equals("int")) {
        return new IntFieldSource(name);
      } else if (fd.valueType.equals("float")) {
        return new FloatFieldSource(name);
      } else if (fd.valueType.equals("long")) {
        return new LongFieldSource(name);
      } else if (fd.valueType.equals("double")) {
        return new DoubleFieldSource(name);
      } else {
        assert false: "unknown numeric field type: " + fd.valueType;
        return null;
      }
    } else {
      throw new IllegalArgumentException("Field \'" + name + "\' cannot be used in an expression: it was not registered with sort=true");
    }
  }
}
