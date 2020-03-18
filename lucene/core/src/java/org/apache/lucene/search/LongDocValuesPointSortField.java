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

package org.apache.lucene.search;

/*
 * Sort field for long values indexed both with doc values and points.
 * Use this field if you want to skip collecting non-competitive documents,
 * which in some cases can significantly speed up sort queries.
 */
public class LongDocValuesPointSortField extends SortField {

    LongDocValuesPointSortField(String field) {
        super(field, SortField.Type.CUSTOM);
    }

    LongDocValuesPointSortField(String field, boolean reverse) {
        super(field, SortField.Type.CUSTOM, reverse);
    }

    @Override
    public FieldComparator<?> getComparator(int numHits, int sortPos) {
        if (sortPos == 0) {
            return new LongDocValuesPointComparator(getField(), numHits, reverse, (Long) missingValue);
        } else {
            return new FieldComparator.LongComparator(numHits, getField(), (Long) missingValue);
        }
    }

    @Override
    public void setMissingValue(Object missingValue) {
        if (missingValue != null && missingValue.getClass() != Long.class)
            throw new IllegalArgumentException("Missing values for Type.LONG can only be of type java.lang.Long, but got " + missingValue.getClass());
        this.missingValue = missingValue;
    }
}
