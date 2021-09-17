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
package org.apache.solr.handler.component;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.search.SortSpec;
import org.mockito.Mockito;

public class MockSortSpecBuilder {
    private final SortSpec sortSpec;

    public MockSortSpecBuilder() {
        this.sortSpec = Mockito.mock(SortSpec.class);
        Mockito.when(sortSpec.getCount()).thenReturn(10);
    }

    public static MockSortSpecBuilder create() {
        return new MockSortSpecBuilder();
    }

    public MockSortSpecBuilder withSortFields(SortField[] sortFields) {
        Sort sort = Mockito.mock(Sort.class);
        Mockito.when(sort.getSort()).thenReturn(sortFields);
        Mockito.when(sortSpec.getSort()).thenReturn(sort);
        return this;
    }

    public MockSortSpecBuilder withIncludesNonScoreOrDocSortField(boolean include) {
        Mockito.when(sortSpec.includesNonScoreOrDocField()).thenReturn(include);
        return this;
    }

    public SortSpec build() {
        return sortSpec;
    }

}
