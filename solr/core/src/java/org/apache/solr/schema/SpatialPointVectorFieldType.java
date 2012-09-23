package org.apache.solr.schema;

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

import org.apache.lucene.spatial.vector.PointVectorStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @see PointVectorStrategy
 * @lucene.experimental
 */
public class SpatialPointVectorFieldType extends AbstractSpatialFieldType<PointVectorStrategy> implements SchemaAware {

  protected String numberFieldName = "tdouble";//in example schema defaults to non-zero precision step -- a good choice
  private int precisionStep;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    String v = args.remove( "numberType" );
    if( v != null ) {
      numberFieldName = v;
    }

  }

  @Override
  public void inform(IndexSchema schema) {
    FieldType fieldType = schema.getFieldTypeByName(numberFieldName);
    if( fieldType == null ) {
      throw new RuntimeException( "Can not find number field: "+ numberFieldName);
    }
    //TODO support other numeric types in the future
    if( !(fieldType instanceof TrieDoubleField) ) {
      throw new RuntimeException( "field type must be TrieDoubleField: "+ fieldType);
    }
    precisionStep = ((TrieField)fieldType).getPrecisionStep();

    //Just set these, delegate everything else to the field type
    final int p = (INDEXED | TOKENIZED | OMIT_NORMS | OMIT_TF_POSITIONS);
    List<SchemaField> newFields = new ArrayList<SchemaField>();
    for( SchemaField sf : schema.getFields().values() ) {
      if( sf.getType() == this ) {
        String name = sf.getName();
        newFields.add(new SchemaField(name + PointVectorStrategy.SUFFIX_X, fieldType, p, null));
        newFields.add(new SchemaField(name + PointVectorStrategy.SUFFIX_Y, fieldType, p, null));
      }
    }
    for (SchemaField newField : newFields) {
      schema.getFields().put(newField.getName(), newField);
    }
  }

  @Override
  protected PointVectorStrategy newSpatialStrategy(String fieldName) {
    PointVectorStrategy strategy = new PointVectorStrategy(ctx, fieldName);
    strategy.setPrecisionStep(precisionStep);
    return strategy;
  }

}

