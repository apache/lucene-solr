package org.apache.lucene.benchmark.standard;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * @deprecated Use Task based benchmarker
 *
 **/
public class ReutersQueries
{
    public static String [] STANDARD_QUERIES = {
            //Start with some short queries
            "Salomon", "Comex", "night trading", "Japan Sony",
            //Try some Phrase Queries
            "\"Sony Japan\"", "\"food needs\"~3",
            "\"World Bank\"^2 AND Nigeria", "\"World Bank\" -Nigeria",
            "\"Ford Credit\"~5",
            //Try some longer queries
            "airline Europe Canada destination",
            "Long term pressure by trade " +
                    "ministers is necessary if the current Uruguay round of talks on " +
                    "the General Agreement on Trade and Tariffs (GATT) is to " +
                    "succeed"
    };

    public static Query[] getPrebuiltQueries(String field)
    {
        //be wary of unanalyzed text
        return new Query[]{
                new SpanFirstQuery(new SpanTermQuery(new Term(field, "ford")), 5),
                new SpanNearQuery(new SpanQuery[]{new SpanTermQuery(new Term(field, "night")), new SpanTermQuery(new Term(field, "trading"))}, 4, false),
                new SpanNearQuery(new SpanQuery[]{new SpanFirstQuery(new SpanTermQuery(new Term(field, "ford")), 10), new SpanTermQuery(new Term(field, "credit"))}, 10, false),
                new WildcardQuery(new Term(field, "fo*")),
        };
    }
}
