package org.apache.lucene.swing.models;

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

import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Hits;
import org.apache.lucene.queryParser.MultiFieldQueryParser;

import javax.swing.*;
import javax.swing.event.ListDataListener;
import javax.swing.event.ListDataEvent;
import java.util.ArrayList;

/**
 * See table searcher explanation.
 *
 */
public class ListSearcher extends AbstractListModel {
    private ListModel listModel;

    /**
     * The reference links between the decorated ListModel
     * and this list model based on search criteria
     */
    private ArrayList rowToModelIndex = new ArrayList();

    /**
     * In memory lucene index
     */
    private RAMDirectory directory;

    /**
     * Cached lucene analyzer
     */
    private Analyzer analyzer;

    /**
     * Links between this list model and the decorated list model
     * are maintained through links based on row number. This is a
     * key constant to denote "row number" for indexing
     */
    private static final String ROW_NUMBER = "ROW_NUMBER";

    /**
     * Since we only have one field, unlike lists with multiple
     * fields -- we are just using a constant to denote field name.
     * This is most likely unnecessary and should be removed at
     * a later date
     */
    private static final String FIELD_NAME = "FIELD_NAME";

    /**
     * Cache the current search String. Also used internally to
     * key whether there is an active search running or not. i.e. if
     * searchString is null, there is no active search.
     */
    private String searchString = null;
    private ListDataListener listModelListener;

    public ListSearcher(ListModel newModel) {
        analyzer = new WhitespaceAnalyzer();
        setListModel(newModel);
        listModelListener = new ListModelHandler();
        newModel.addListDataListener(listModelListener);
        clearSearchingState();
    }

    private void setListModel(ListModel newModel) {
        //remove listeners if there...
        if (newModel != null) {
            newModel.removeListDataListener(listModelListener);
        }

        listModel = newModel;
        if (listModel != null) {
            listModel.addListDataListener(listModelListener);
        }

        //recalculate the links between this list model and
        //the inner list model since the decorated model just changed
        reindex();

        // let all listeners know the list has changed
        fireContentsChanged(this, 0, getSize());
    }

    private void reindex() {
        try {
            // recreate the RAMDirectory
            directory = new RAMDirectory();
            IndexWriter writer = new IndexWriter(directory, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);

            // iterate through all rows
            for (int row=0; row < listModel.getSize(); row++){

                //for each row make a new document
                Document document = new Document();
                //add the row number of this row in the decorated list model
                //this will allow us to retrive the results later
                //and map this list model's row to a row in the decorated
                //list model
                document.add(new Field(ROW_NUMBER, "" + row, Field.Store.YES, Field.Index.ANALYZED));
                //add the string representation of the row to the index
                document.add(new Field(FIELD_NAME, String.valueOf(listModel.getElementAt(row)).toLowerCase(), Field.Store.YES, Field.Index.ANALYZED));
                writer.addDocument(document);
            }
            writer.optimize();
            writer.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Run a new search.
     *
     * @param searchString Any valid lucene search string
     */
    public void search(String searchString){

        //if search string is null or empty, clear the search == search all
        if (searchString == null || searchString.equals("")){
            clearSearchingState();
            fireContentsChanged(this, 0, getSize());
            return;
        }


        try {
            //cache search String
            this.searchString = searchString;

            //make a new index searcher with the in memory (RAM) index.
            IndexSearcher is = new IndexSearcher(directory);

            //make an array of fields - one for each column
            String[] fields = {FIELD_NAME};

            //build a query based on the fields, searchString and cached analyzer
            //NOTE: This is an area for improvement since the MultiFieldQueryParser
            // has some weirdness.
            MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, analyzer);
            Query query =parser.parse(searchString);
            //run the search
            Hits hits = is.search(query);
            //reset this list model with the new results
            resetSearchResults(hits);
        } catch (Exception e){
            e.printStackTrace();
        }

        //notify all listeners that the list has been changed
        fireContentsChanged(this, 0, getSize());
    }

    /**
     *
     * @param hits The new result set to set this list to.
     */
    private void resetSearchResults(Hits hits) {
        try {
            //clear our index mapping this list model rows to
            //the decorated inner list model
            rowToModelIndex.clear();
            //iterate through the hits
            //get the row number stored at the index
            //that number is the row number of the decorated
            //tabble model row that we are mapping to
            for (int t=0; t<hits.length(); t++){
                Document document = hits.doc(t);
                Fieldable field = document.getField(ROW_NUMBER);
                rowToModelIndex.add(new Integer(field.stringValue()));
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * @return The current lucene analyzer
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /**
     * @param analyzer The new analyzer to use
     */
    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
        //reindex from the model with the new analyzer
        reindex();

        //rerun the search if there is an active search
        if (isSearching()){
            search(searchString);
        }
    }

    private boolean isSearching() {
        return searchString != null;
    }

    private void clearSearchingState() {
        searchString = null;
        rowToModelIndex.clear();
        for (int t=0; t<listModel.getSize(); t++){
            rowToModelIndex.add(new Integer(t));
        }
    }

    private int getModelRow(int row){
        return ((Integer) rowToModelIndex.get(row)).intValue();
    }

    public int getSize() {
        return (listModel == null) ? 0 : rowToModelIndex.size();
    }

    public Object getElementAt(int index) {
        return listModel.getElementAt(getModelRow(index));
    }


    class ListModelHandler implements ListDataListener {

        public void contentsChanged(ListDataEvent e) {
            somethingChanged();
        }

        public void intervalAdded(ListDataEvent e) {
            somethingChanged();
        }

        public void intervalRemoved(ListDataEvent e) {
            somethingChanged();
        }

        private void somethingChanged(){
            // If we're not searching, just pass the event along.
            if (!isSearching()) {
                clearSearchingState();
                reindex();
                fireContentsChanged(ListSearcher.this, 0, getSize());
                return;
            }

            // Something has happened to the data that may have invalidated the search.
            reindex();
            search(searchString);
            fireContentsChanged(ListSearcher.this, 0, getSize());
            return;
        }

    }


}
