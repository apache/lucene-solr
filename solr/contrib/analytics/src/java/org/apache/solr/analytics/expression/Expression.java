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
package org.apache.solr.analytics.expression;

import java.util.Comparator;

import org.apache.solr.analytics.request.FieldFacetRequest.FacetSortDirection;

/**
 * Expressions map either zero, one, two or many inputs to a single value. 
 * They can be defined recursively to compute complex math.
 */
public abstract class Expression {
  public abstract Comparable getValue();

  public Comparator<Expression> comparator(final FacetSortDirection direction) {
    return (a, b) -> {
      boolean aIsNull = a.getValue() == null;
      boolean bIsNull = b.getValue() == null;

      if (aIsNull && bIsNull) return 0;

      if( direction == FacetSortDirection.ASCENDING ){ // nulls are last for ASC sort
        return aIsNull ? 1
          : bIsNull ? -1
          : a.getValue().compareTo(b.getValue());
      } else {
        return aIsNull ? -1
          : bIsNull ? 1
          : b.getValue().compareTo(a.getValue());
      }
    };
  }
}
