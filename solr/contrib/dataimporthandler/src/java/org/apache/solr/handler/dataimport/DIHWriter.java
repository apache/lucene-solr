package org.apache.solr.handler.dataimport;

import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;

/**
 * @solr.experimental
 *
 */
public interface DIHWriter {
	
	/**
	 * <p>
	 *  If this writer supports transactions or commit points, then commit any changes, 
	 *  optionally optimizing the data for read/write performance
	 * </p>
	 * @param optimize
	 */
	public void commit(boolean optimize);
	
	/**
	 * <p>
	 *  Release resources used by this writer.  After calling close, reads & updates will throw exceptions.
	 * </p>
	 */
	public void close();

	/**
	 * <p>
	 *  If this writer supports transactions or commit points, then roll back any uncommitted changes.
	 * </p>
	 */
	public void rollback();

	/**
	 * <p>
	 *  Delete from the writer's underlying data store based the passed-in writer-specific query. (Optional Operation)
	 * </p>
	 * @param q
	 */
	public void deleteByQuery(String q);

	/**
	 * <p>
	 *  Delete everything from the writer's underlying data store
	 * </p>
	 */
	public void doDeleteAll();

	/**
	 * <p>
	 *  Delete from the writer's underlying data store based on the passed-in Primary Key
	 * </p>
	 * @param key
	 */
	public void deleteDoc(Object key);
	


	/**
	 * <p>
	 *  Add a document to this writer's underlying data store.
	 * </p>
	 * @param doc
	 * @return
	 */
	public boolean upload(SolrInputDocument doc);


	
	/**
	 * <p>
	 *  Provide context information for this writer.  init() should be called before using the writer.
	 * </p>
	 * @param context
	 */
	public void init(Context context) ;
	
}
