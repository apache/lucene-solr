package org.apache.lucene.facet.example.association;

import org.apache.lucene.facet.associations.CategoryAssociation;
import org.apache.lucene.facet.associations.CategoryFloatAssociation;
import org.apache.lucene.facet.associations.CategoryIntAssociation;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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

/**
 * @lucene.experimental
 */
public class CategoryAssociationsUtils {

  /**
   * Categories: categories[D][N] == category-path with association no. N for
   * document no. D.
   */
  public static CategoryPath[][] categories = {
    // Doc #1
    { new CategoryPath("tags", "lucene") , 
      new CategoryPath("genre", "computing")
    },
        
    // Doc #2
    { new CategoryPath("tags", "lucene"),  
      new CategoryPath("tags", "solr"),
      new CategoryPath("genre", "computing"),
      new CategoryPath("genre", "software")
    }
  };

  public static CategoryAssociation[][] associations = {
    // Doc #1 associations
    {
      /* 3 occurrences for tag 'lucene' */
      new CategoryIntAssociation(3), 
      /* 87% confidence level of genre 'computing' */
      new CategoryFloatAssociation(0.87f)
    },
    
    // Doc #2 associations
    {
      /* 1 occurrence for tag 'lucene' */
      new CategoryIntAssociation(1),
      /* 2 occurrences for tag 'solr' */
      new CategoryIntAssociation(2),
      /* 75% confidence level of genre 'computing' */
      new CategoryFloatAssociation(0.75f),
      /* 34% confidence level of genre 'software' */
      new CategoryFloatAssociation(0.34f),
    }
  };

}
