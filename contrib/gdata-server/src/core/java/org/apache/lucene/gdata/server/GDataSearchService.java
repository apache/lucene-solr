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

package org.apache.lucene.gdata.server;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.search.GDataSearcher;
import org.apache.lucene.gdata.search.SearchComponent;
import org.apache.lucene.gdata.search.query.GDataQueryParser;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;

/**
 * @author Simon Willnauer
 *
 */
public class GDataSearchService extends GDataService {
    private static final Log LOG = LogFactory.getLog(GDataSearchService.class);
    private static SearchComponent SEARCHCOMPONENT;
    private GDataSearcher<String> searcher;
    protected GDataSearchService() throws ServiceException {
        if(SEARCHCOMPONENT == null)
            SEARCHCOMPONENT = GDataServerRegistry.getRegistry().lookup(SearchComponent.class,ComponentType.SEARCHCONTROLLER);
        
       
    }

    /**
     * @see org.apache.lucene.gdata.server.GDataService#getFeed(org.apache.lucene.gdata.server.GDataRequest, org.apache.lucene.gdata.server.GDataResponse)
     */
    @SuppressWarnings("unchecked")
    @Override
    public BaseFeed getFeed(GDataRequest request, GDataResponse response) throws ServiceException {
        String translatedQuery = request.getTranslatedQuery();
        ProvidedService service = request.getConfigurator();
        
        QueryParser parser = new GDataQueryParser(service.getIndexSchema());
        Query query;
        try {
            query = parser.parse(translatedQuery);
           
        } catch (ParseException e1) {
            throw new ServiceException("Search Failed -- Can not parse query",e1,GDataResponse.BAD_REQUEST);
        }
        if(LOG.isInfoEnabled())
            LOG.info("Fire search for user query  query: "+query.toString());
        this.searcher = SEARCHCOMPONENT.getServiceSearcher(service);
        List<String> result;
        try {
            result = this.searcher.search(query,request.getItemsPerPage(),request.getStartIndex(),request.getFeedId());
        } catch (IOException e) {
           throw new ServiceException("Search Failed -- Searcher throws IOException",e,GDataResponse.SERVER_ERROR); 
           
        }
        if(LOG.isInfoEnabled())
            LOG.info("Fetching results for user query result size: "+result.size());
        ServerBaseFeed requestFeed = new ServerBaseFeed();
        requestFeed.setServiceConfig(service);
        requestFeed.setStartIndex(0);
        requestFeed.setItemsPerPage(0);
        requestFeed.setId(request.getFeedId());
        BaseFeed feed = null;
        try{
         feed = this.storage.getFeed(requestFeed);
        }catch (StorageException e) {
            throw new ServiceException("Search Failed -- can not get feed, feed not stored ",e,GDataResponse.NOT_FOUND);
        }
        for (String entryId : result) {
            ServerBaseEntry requestEntry = new ServerBaseEntry();
            requestEntry.setId(entryId);
            requestEntry.setServiceConfig(service);
            try{
            BaseEntry entry = this.storage.getEntry(requestEntry);
            feed.getEntries().add(entry);
            }catch (StorageException e) {
                
                LOG.error("StorageException caught while fetching query results -- skip entry -- "+e.getMessage(),e);
            }
        }
        dynamicElementFeedStragey(feed,request);
        return feed;
    }

    

}
