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

package org.apache.solr.handler.sql.functions;

/**
 * Operator for filtering on Solr multi-valued fields with 'AND" clause.
 * Example: ARRAY_CONTAINS_ALL(field, ('val1', 'val2')) will be transformed to
 * filter query field:("val1" AND "val2")
 */
public class ArrayContainsAll extends ArrayContains {
    private static final String UDF_NAME = "ARRAY_CONTAINS_ALL";

    public ArrayContainsAll() {
        super(UDF_NAME);
    }

    @Override
    public String getAllowedSignatures(String opNameToUse) {
        return "ARRAY_CONTAINS_ALL(IDENTIFIER, ('val1', 'val2'))";
    }
}
