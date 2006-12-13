/**
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

package org.apache.lucene.gdata.search.query;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.gdata.search.config.IndexSchema;

import com.google.gdata.data.DateTime;

/**
 * Simple static methods to translate the http query to a lucene query string.
 * @author Simon Willnauer
 * 
 */
public class QueryTranslator {
    private static final Set<String> STANDARD_REQUEST_PARAMETER = new HashSet<String>(3);
    private static final String GDATA_DEFAULT_SEARCH_PARAMETER = "q";
    private static final String UPDATED_MIN = Long.toString(0);
    private static final String UPDATED_MAX = Long.toString(Long.MAX_VALUE);
//    private static final String GDATA_CATEGORY_FIEL = 

    
    
    static{
        STANDARD_REQUEST_PARAMETER.add("max-results");
        STANDARD_REQUEST_PARAMETER.add("start-index");
        STANDARD_REQUEST_PARAMETER.add("alt");
    }
    /**
     * This method does a little preprocessing of the query. Basically it will map the given request parameters to a lucene syntax. Each
     * parameter matching a index field in the given schema will be translated into a grouped query string according to the lucene query syntax. 
     * <p>
     * <ol>
     * <li>title=foo bar AND "FooBar" will be title:(foo bar AND "FooBar)</i>
     * <li>updated-min=2005-08-09T10:57:00-08:00 will be translated to updated:[1123613820000 TO 9223372036854775807] according to the gdata protocol</i>
     * </ol>
     * </p>
     * @param schema the index schema for the queried service
     * @param parameterMap - the http parameter map returning String[] instances as values
     * @param categoryQuery - the parsed category query from the request
     * @return - a lucene syntax query string
     */
    public static String translateHttpSearchRequest(IndexSchema schema,
            Map<String, String[]> parameterMap, String categoryQuery) {
        Set<String> searchableFieldNames = schema.getSearchableFieldNames();
        Set<String> parameterSet = parameterMap.keySet();
        StringBuilder translatedQuery = new StringBuilder();
        if(categoryQuery != null){
           translatedQuery.append(translateCategory(translatedQuery,categoryQuery));
        }
        String updateMin = null;
        String updateMax = null;
        for (String parameterName : parameterSet) {
            if (STANDARD_REQUEST_PARAMETER.contains(parameterName))
                continue;
            if (searchableFieldNames.contains(parameterName)) {
                translatedQuery.append(parameterName).append(":(");
                translatedQuery.append(parameterMap.get(parameterName)[0]);
                translatedQuery.append(") ");
                continue;
            }
            if(parameterName.equals(GDATA_DEFAULT_SEARCH_PARAMETER)){
                translatedQuery.append(schema.getDefaultSearchField());
                translatedQuery.append(":(");
                translatedQuery.append(parameterMap.get(parameterName)[0]);
                translatedQuery.append(") ");
                continue;
                
            }
            if(parameterName.endsWith("updated-min")){
                updateMin = parameterMap.get(parameterName)[0];
                continue;
            }
            if(parameterName.endsWith("updated-max")){
                updateMax = parameterMap.get(parameterName)[0];
                continue;
            }
            throw new RuntimeException("Can not apply parameter -- invalid -- "
                    + parameterName);
        }
        if(updateMax!=null || updateMin!= null)
            translatedQuery.append(translateUpdate(updateMin,updateMax));
            
        return translatedQuery.length() == 0?null:translatedQuery.toString();
    }
    
    
     static String translateUpdate(String updateMin, String updateMax){
        StringBuilder builder = new StringBuilder("updated:[");
        if(updateMin != null)
            builder.append(Long.toString(DateTime.parseDateTime(updateMin).getValue()));
        else
            builder.append(UPDATED_MIN);
        builder.append(" TO ");
        if(updateMax != null)
            builder.append(Long.toString(DateTime.parseDateTime(updateMax).getValue()-1));
        else
            builder.append(UPDATED_MAX);
        builder.append("]");
        return builder.toString();
        
        
    }
     
     static String translateCategory(StringBuilder builder, String categoryQuery){
         return categoryQuery;
         //TODO Implement this
//         GDataCategoryQueryParser parser = new GDataCategoryQueryParser()
         
        
     }
     
     
     
    
    
}
