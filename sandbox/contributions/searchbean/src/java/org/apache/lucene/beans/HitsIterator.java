/*
 * HitsIterator.java
 * Provides an Iterator class around Lucene Hits
 * It also supports paging
 * Created on November 1, 2001, 8:53 PM
 */

package org.apache.lucene.beans;

import org.apache.lucene.beans.SortedField;
import org.apache.lucene.beans.CompareDocumentsByField;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.Hits;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

//import org.apache.log4j.Logger;

/**
 *
 * @author  Peter Carlson
 * @version 1.0
 */
public class HitsIterator {

    //static Logger logger = Logger.getLogger(HitsIterator.class.getName());

    private int currentPosition = 0;

    private Hits hitsCollection = null;
    private Object[] arrayOfIndividualHits = null;

    private int totalHits = 0;

    private int pageSize = 25; // default page size

    private int currentPage = 1; // range from 1 to totalHits%pageSize

    private int totalPages = -1; // set by constructor

    private int endPagePosition = 0; // position currentPage ends

    /** Creates new HitsIterator */
    private HitsIterator() {
    }

    public HitsIterator(Hits hits) throws IOException{
        this(hits,null);
    }

    public HitsIterator(Hits hits, String sortFlag) throws IOException{
        this.hitsCollection = hits;
        if (sortFlag != null){
            if (sortFlag != "") {
                System.out.println("Sorting hits by field "+sortFlag);
                sortByField(sortFlag);
                //logger.debug("Completed sorting by field "+sortFlag);
            }
        }
        totalHits = getTotalHits();
        setPageCount();
    }

    /** sorts hits by the given sort flag
     * fills an interal array
     * @param sortFlag field to sort results on
     */
    private void sortByField(String fieldName) throws IOException{
        long start = System.currentTimeMillis();
        Comparator c = null;
        if (fieldName == null){
            //logger.error("sort field is null");
            return;
        }

        SortedField sf = SortedField.getSortedField(fieldName);
        if (sf !=null){
            c = (Comparator) new CompareDocumentsByField();
        } else {
            //logger.error("Sort field not found");
            // use default sort of Lucene -- Relevance
            // Should I throw an exception here?
            arrayOfIndividualHits = null;
            return;
        }
        arrayOfIndividualHits = new Object[hitsCollection.length()];
        long first = System.currentTimeMillis();
        for (int i=0; i<hitsCollection.length(); i++) {
            int id = hitsCollection.id(i);
            arrayOfIndividualHits[i] = new IndividualHit(i, sf.getFieldValue(id), hitsCollection.score(i));
        }
        long second = System.currentTimeMillis();
        //logger.debug("HitsIterator.sortByField(): filling Obj[] took "+(second-first));

        Arrays.sort(arrayOfIndividualHits, c);
        //logger.debug("HitsIterator.sortByField(): sort took "+(System.currentTimeMillis()-second));

    }


    private void setPageCount() {
        if (totalHits == 0){
            totalPages = 0;
            setCurrentPage(0);
        } else {
            totalPages = totalHits / pageSize;

            //account for remainder if not exaxtly divisable
            if (totalHits % pageSize != 0)
            { totalPages++;}
            setCurrentPage(1); // reset currentPage to make sure not over the limit
        }
    }

    public int getPageCount() {
        return totalPages;
    }


    public org.apache.lucene.document.Document setPosition(int position) throws IOException{
        if (position > totalHits) {
            return null;
        }
        currentPosition = position;
        return getDoc();
    }

    public org.apache.lucene.document.Document next() throws IOException{
        currentPosition++;

        if (currentPosition > totalHits) {
            currentPosition = totalHits;
            return null ;
        }

        return getDoc();
    }

    public org.apache.lucene.document.Document previous() throws IOException{
        currentPosition--;

        if (currentPosition < 0)
        { return null;}

        return getDoc();
    }

    public boolean hasNext() {
        if (currentPosition < endPagePosition)
        { return true; }

        return false;
    }

    public org.apache.lucene.document.Document getDoc() throws IOException {
        // Determine if using relevnace or sorting by another field
        if (arrayOfIndividualHits == null)
            return hitsCollection.doc(currentPosition - 1);
        else {
            int i = ((IndividualHit)arrayOfIndividualHits[currentPosition - 1]).getIndex();
            return hitsCollection.doc(i);
        }
    }

    public int getScore() throws Exception{
        // Determine if using relevnace or sorting by another field
        if (arrayOfIndividualHits == null)
            return (int) (hitsCollection.score(currentPosition - 1)*100.0f);
        else
            return (int) (((IndividualHit)arrayOfIndividualHits[currentPosition - 1]).getScore()*100.0f);
    }

    public int getTotalHits() {
        return hitsCollection.length();
    }

    public int getCurrentPosition() {
        return currentPosition;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
        setPageCount();
    }

    public void setCurrentPage(int currentPage) throws IndexOutOfBoundsException{
        if (currentPage > totalPages){
            currentPage = totalPages; // don't allow to go over max
            //throw new IndexOutOfBoundsException("currentPage greater than total pages");
        }

        this.currentPage = currentPage;
        currentPosition = ((currentPage - 1) * pageSize);
        endPagePosition = Math.min( ((currentPage - 1)*pageSize) + pageSize, totalHits);
    }

    public int getCurrentPage() {
        return currentPage;
    }

    /**
     * set page number to next page, unless last page, then
     * always return last page number
     *@return current page number
     */
    public int nextPage() {
        setCurrentPage(currentPage++);
        return getCurrentPage();
    }

    /**
     * set page number to previous page, unless first page,
     * then always return first page number
     *@return current page number
     */
    public int previousPage() {
        setCurrentPage(currentPage--);
        return getCurrentPage();
    }
}
