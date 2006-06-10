package org.apache.lucene.index;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

/**
 * A class to modify an index, i.e. to delete and add documents. This
 * class hides {@link IndexReader} and {@link IndexWriter} so that you
 * do not need to care about implementation details such as that adding
 * documents is done via IndexWriter and deletion is done via IndexReader.
 * 
 * <p>Note that you cannot create more than one <code>IndexModifier</code> object
 * on the same directory at the same time.
 * 
 * <p>Example usage:
 * 
<!-- ======================================================== -->
<!-- = Java Sourcecode to HTML automatically converted code = -->
<!-- =   Java2Html Converter V4.1 2004 by Markus Gebhard  markus@jave.de   = -->
<!-- =     Further information: http://www.java2html.de     = -->
<div align="left" class="java">
<table border="0" cellpadding="3" cellspacing="0" bgcolor="#ffffff">
   <tr>
  <!-- start source code -->
   <td nowrap="nowrap" valign="top" align="left">
    <code>
<font color="#ffffff">&nbsp;&nbsp;&nbsp;&nbsp;</font><font color="#000000">Analyzer&nbsp;analyzer&nbsp;=&nbsp;</font><font color="#7f0055"><b>new&nbsp;</b></font><font color="#000000">StandardAnalyzer</font><font color="#000000">()</font><font color="#000000">;</font><br/>
<font color="#ffffff">&nbsp;&nbsp;&nbsp;&nbsp;</font><font color="#3f7f5f">//&nbsp;create&nbsp;an&nbsp;index&nbsp;in&nbsp;/tmp/index,&nbsp;overwriting&nbsp;an&nbsp;existing&nbsp;one:</font><br/>
<font color="#ffffff">&nbsp;&nbsp;&nbsp;&nbsp;</font><font color="#000000">IndexModifier&nbsp;indexModifier&nbsp;=&nbsp;</font><font color="#7f0055"><b>new&nbsp;</b></font><font color="#000000">IndexModifier</font><font color="#000000">(</font><font color="#2a00ff">&#34;/tmp/index&#34;</font><font color="#000000">,&nbsp;analyzer,&nbsp;</font><font color="#7f0055"><b>true</b></font><font color="#000000">)</font><font color="#000000">;</font><br/>
<font color="#ffffff">&nbsp;&nbsp;&nbsp;&nbsp;</font><font color="#000000">Document&nbsp;doc&nbsp;=&nbsp;</font><font color="#7f0055"><b>new&nbsp;</b></font><font color="#000000">Document</font><font color="#000000">()</font><font color="#000000">;</font><br/>
<font color="#ffffff">&nbsp;&nbsp;&nbsp;&nbsp;</font><font color="#000000">doc.add</font><font color="#000000">(</font><font color="#7f0055"><b>new&nbsp;</b></font><font color="#000000">Field</font><font color="#000000">(</font><font color="#2a00ff">&#34;id&#34;</font><font color="#000000">,&nbsp;</font><font color="#2a00ff">&#34;1&#34;</font><font color="#000000">,&nbsp;Field.Store.YES,&nbsp;Field.Index.UN_TOKENIZED</font><font color="#000000">))</font><font color="#000000">;</font><br/>
<font color="#ffffff">&nbsp;&nbsp;&nbsp;&nbsp;</font><font color="#000000">doc.add</font><font color="#000000">(</font><font color="#7f0055"><b>new&nbsp;</b></font><font color="#000000">Field</font><font color="#000000">(</font><font color="#2a00ff">&#34;body&#34;</font><font color="#000000">,&nbsp;</font><font color="#2a00ff">&#34;a&nbsp;simple&nbsp;test&#34;</font><font color="#000000">,&nbsp;Field.Store.YES,&nbsp;Field.Index.TOKENIZED</font><font color="#000000">))</font><font color="#000000">;</font><br/>
<font color="#ffffff">&nbsp;&nbsp;&nbsp;&nbsp;</font><font color="#000000">indexModifier.addDocument</font><font color="#000000">(</font><font color="#000000">doc</font><font color="#000000">)</font><font color="#000000">;</font><br/>
<font color="#ffffff">&nbsp;&nbsp;&nbsp;&nbsp;</font><font color="#7f0055"><b>int&nbsp;</b></font><font color="#000000">deleted&nbsp;=&nbsp;indexModifier.delete</font><font color="#000000">(</font><font color="#7f0055"><b>new&nbsp;</b></font><font color="#000000">Term</font><font color="#000000">(</font><font color="#2a00ff">&#34;id&#34;</font><font color="#000000">,&nbsp;</font><font color="#2a00ff">&#34;1&#34;</font><font color="#000000">))</font><font color="#000000">;</font><br/>
<font color="#ffffff">&nbsp;&nbsp;&nbsp;&nbsp;</font><font color="#000000">System.out.println</font><font color="#000000">(</font><font color="#2a00ff">&#34;Deleted&nbsp;&#34;&nbsp;</font><font color="#000000">+&nbsp;deleted&nbsp;+&nbsp;</font><font color="#2a00ff">&#34;&nbsp;document&#34;</font><font color="#000000">)</font><font color="#000000">;</font><br/>
<font color="#ffffff">&nbsp;&nbsp;&nbsp;&nbsp;</font><font color="#000000">indexModifier.flush</font><font color="#000000">()</font><font color="#000000">;</font><br/>
<font color="#ffffff">&nbsp;&nbsp;&nbsp;&nbsp;</font><font color="#000000">System.out.println</font><font color="#000000">(</font><font color="#000000">indexModifier.docCount</font><font color="#000000">()&nbsp;</font><font color="#000000">+&nbsp;</font><font color="#2a00ff">&#34;&nbsp;docs&nbsp;in&nbsp;index&#34;</font><font color="#000000">)</font><font color="#000000">;</font><br/>
<font color="#ffffff">&nbsp;&nbsp;&nbsp;&nbsp;</font><font color="#000000">indexModifier.close</font><font color="#000000">()</font><font color="#000000">;</font></code>

   </td>
  <!-- end source code -->
   </tr>
</table>
</div>
<!-- =       END of automatically generated HTML code       = -->
<!-- ======================================================== -->
 *
 * <p>Not all methods of IndexReader and IndexWriter are offered by this
 * class. If you need access to additional methods, either use those classes
 * directly or implement your own class that extends <code>IndexModifier</code>.
 *
 * <p>Although an instance of this class can be used from more than one
 * thread, you will not get the best performance. You might want to use
 * IndexReader and IndexWriter directly for that (but you will need to
 * care about synchronization yourself then).
 *
 * <p>While you can freely mix calls to add() and delete() using this class,
 * you should batch you calls for best performance. For example, if you
 * want to update 20 documents, you should first delete all those documents,
 * then add all the new documents.
 *
 * @author Daniel Naber
 */
public class IndexModifier {

  protected IndexWriter indexWriter = null;
  protected IndexReader indexReader = null;

  protected Directory directory = null;
  protected Analyzer analyzer = null;
  protected boolean open = false;

  // Lucene defaults:
  protected PrintStream infoStream = null;
  protected boolean useCompoundFile = true;
  protected int maxBufferedDocs = IndexWriter.DEFAULT_MAX_BUFFERED_DOCS;
  protected int maxFieldLength = IndexWriter.DEFAULT_MAX_FIELD_LENGTH;
  protected int mergeFactor = IndexWriter.DEFAULT_MERGE_FACTOR;

  /**
   * Open an index with write access.
   *
   * @param directory the index directory
   * @param analyzer the analyzer to use for adding new documents
   * @param create <code>true</code> to create the index or overwrite the existing one;
   * 	<code>false</code> to append to the existing index
   */
  public IndexModifier(Directory directory, Analyzer analyzer, boolean create) throws IOException {
    init(directory, analyzer, create);
  }

  /**
   * Open an index with write access.
   *
   * @param dirName the index directory
   * @param analyzer the analyzer to use for adding new documents
   * @param create <code>true</code> to create the index or overwrite the existing one;
   * 	<code>false</code> to append to the existing index
   */
  public IndexModifier(String dirName, Analyzer analyzer, boolean create) throws IOException {
    Directory dir = FSDirectory.getDirectory(dirName, create);
    init(dir, analyzer, create);
  }

  /**
   * Open an index with write access.
   *
   * @param file the index directory
   * @param analyzer the analyzer to use for adding new documents
   * @param create <code>true</code> to create the index or overwrite the existing one;
   * 	<code>false</code> to append to the existing index
   */
  public IndexModifier(File file, Analyzer analyzer, boolean create) throws IOException {
    Directory dir = FSDirectory.getDirectory(file, create);
    init(dir, analyzer, create);
  }

  /**
   * Initialize an IndexWriter.
   * @throws IOException
   */
  protected void init(Directory directory, Analyzer analyzer, boolean create) throws IOException {
    this.directory = directory;
    synchronized(this.directory) {
      this.analyzer = analyzer;
      indexWriter = new IndexWriter(directory, analyzer, create);
      open = true;
    }
  }

  /**
   * Throw an IllegalStateException if the index is closed.
   * @throws IllegalStateException
   */
  protected void assureOpen() {
    if (!open) {
      throw new IllegalStateException("Index is closed");
    }
  }

  /**
   * Close the IndexReader and open an IndexWriter.
   * @throws IOException
   */
  protected void createIndexWriter() throws IOException {
    if (indexWriter == null) {
      if (indexReader != null) {
        indexReader.close();
        indexReader = null;
      }
      indexWriter = new IndexWriter(directory, analyzer, false);
      indexWriter.setInfoStream(infoStream);
      indexWriter.setUseCompoundFile(useCompoundFile);
      indexWriter.setMaxBufferedDocs(maxBufferedDocs);
      indexWriter.setMaxFieldLength(maxFieldLength);
      indexWriter.setMergeFactor(mergeFactor);
    }
  }

  /**
   * Close the IndexWriter and open an IndexReader.
   * @throws IOException
   */
  protected void createIndexReader() throws IOException {
    if (indexReader == null) {
      if (indexWriter != null) {
        indexWriter.close();
        indexWriter = null;
      }
      indexReader = IndexReader.open(directory);
    }
  }

  /**
   * Make sure all changes are written to disk.
   * @throws IOException
   */
  public void flush() throws IOException {
    synchronized(directory) {
      assureOpen();
      if (indexWriter != null) {
        indexWriter.close();
        indexWriter = null;
        createIndexWriter();
      } else {
        indexReader.close();
        indexReader = null;
        createIndexReader();
      }
    }
  }

  /**
   * Adds a document to this index, using the provided analyzer instead of the
   * one specific in the constructor.  If the document contains more than
   * {@link #setMaxFieldLength(int)} terms for a given field, the remainder are
   * discarded.
   * @see IndexWriter#addDocument(Document, Analyzer)
   * @throws IllegalStateException if the index is closed
   */
  public void addDocument(Document doc, Analyzer docAnalyzer) throws IOException {
    synchronized(directory) {
      assureOpen();
      createIndexWriter();
      if (docAnalyzer != null)
        indexWriter.addDocument(doc, docAnalyzer);
      else
        indexWriter.addDocument(doc);
    }
  }

  /**
   * Adds a document to this index.  If the document contains more than
   * {@link #setMaxFieldLength(int)} terms for a given field, the remainder are
   * discarded.
   * @see IndexWriter#addDocument(Document)
   * @throws IllegalStateException if the index is closed
   */
  public void addDocument(Document doc) throws IOException {
    addDocument(doc, null);
  }

  /**
   * Deletes all documents containing <code>term</code>.
   * This is useful if one uses a document field to hold a unique ID string for
   * the document.  Then to delete such a document, one merely constructs a
   * term with the appropriate field and the unique ID string as its text and
   * passes it to this method.  Returns the number of documents deleted.
   * @return the number of documents deleted
   * @see IndexReader#deleteDocuments(Term)
   * @throws IllegalStateException if the index is closed
   */
  public int deleteDocuments(Term term) throws IOException {
    synchronized(directory) {
      assureOpen();
      createIndexReader();
      return indexReader.deleteDocuments(term);
    }
  }

  /**
   * Deletes the document numbered <code>docNum</code>.
   * @see IndexReader#deleteDocument(int)
   * @throws IllegalStateException if the index is closed
   */
  public void deleteDocument(int docNum) throws IOException {
    synchronized(directory) {
      assureOpen();
      createIndexReader();
      indexReader.deleteDocument(docNum);
    }
  }


  /**
   * Returns the number of documents currently in this index.
   * @see IndexWriter#docCount()
   * @see IndexReader#numDocs()
   * @throws IllegalStateException if the index is closed
   */
  public int docCount() {
    synchronized(directory) {
      assureOpen();
      if (indexWriter != null) {
        return indexWriter.docCount();
      } else {
        return indexReader.numDocs();
      }
    }
  }

  /**
   * Merges all segments together into a single segment, optimizing an index
   * for search.
   * @see IndexWriter#optimize()
   * @throws IllegalStateException if the index is closed
   */
  public void optimize() throws IOException {
    synchronized(directory) {
      assureOpen();
      createIndexWriter();
      indexWriter.optimize();
    }
  }

  /**
   * If non-null, information about merges and a message when
   * {@link #getMaxFieldLength()} is reached will be printed to this.
   * <p>Example: <tt>index.setInfoStream(System.err);</tt>
   * @see IndexWriter#setInfoStream(PrintStream)
   * @throws IllegalStateException if the index is closed
   */
  public void setInfoStream(PrintStream infoStream) {
    synchronized(directory) {
      assureOpen();
      if (indexWriter != null) {
        indexWriter.setInfoStream(infoStream);
      }
      this.infoStream = infoStream;
    }
  }

  /**
   * @throws IOException
   * @see IndexModifier#setInfoStream(PrintStream)
   */
  public PrintStream getInfoStream() throws IOException {
    synchronized(directory) {
      assureOpen();
      createIndexWriter();
      return indexWriter.getInfoStream();
    }
  }

  /**
   * Setting to turn on usage of a compound file. When on, multiple files
   * for each segment are merged into a single file once the segment creation
   * is finished. This is done regardless of what directory is in use.
   * @see IndexWriter#setUseCompoundFile(boolean)
   * @throws IllegalStateException if the index is closed
   */
  public void setUseCompoundFile(boolean useCompoundFile) {
    synchronized(directory) {
      assureOpen();
      if (indexWriter != null) {
        indexWriter.setUseCompoundFile(useCompoundFile);
      }
      this.useCompoundFile = useCompoundFile;
    }
  }

  /**
   * @throws IOException
   * @see IndexModifier#setUseCompoundFile(boolean)
   */
  public boolean getUseCompoundFile() throws IOException {
    synchronized(directory) {
      assureOpen();
      createIndexWriter();
      return indexWriter.getUseCompoundFile();
    }
  }

  /**
   * The maximum number of terms that will be indexed for a single field in a
   * document.  This limits the amount of memory required for indexing, so that
   * collections with very large files will not crash the indexing process by
   * running out of memory.<p/>
   * Note that this effectively truncates large documents, excluding from the
   * index terms that occur further in the document.  If you know your source
   * documents are large, be sure to set this value high enough to accomodate
   * the expected size.  If you set it to Integer.MAX_VALUE, then the only limit
   * is your memory, but you should anticipate an OutOfMemoryError.<p/>
   * By default, no more than 10,000 terms will be indexed for a field.
   * @see IndexWriter#setMaxFieldLength(int)
   * @throws IllegalStateException if the index is closed
   */
  public void setMaxFieldLength(int maxFieldLength) {
    synchronized(directory) {
      assureOpen();
      if (indexWriter != null) {
        indexWriter.setMaxFieldLength(maxFieldLength);
      }
      this.maxFieldLength = maxFieldLength;
    }
  }

  /**
   * @throws IOException
   * @see IndexModifier#setMaxFieldLength(int)
   */
  public int getMaxFieldLength() throws IOException {
    synchronized(directory) {
      assureOpen();
      createIndexWriter();
      return indexWriter.getMaxFieldLength();
    }
  }

  /**
   * Determines the minimal number of documents required before the buffered
   * in-memory documents are merging and a new Segment is created.
   * Since Documents are merged in a {@link org.apache.lucene.store.RAMDirectory},
   * large value gives faster indexing.  At the same time, mergeFactor limits
   * the number of files open in a FSDirectory.
   *
   * <p>The default value is 10.
   *
   * @see IndexWriter#setMaxBufferedDocs(int)
   * @throws IllegalStateException if the index is closed
   * @throws IllegalArgumentException if maxBufferedDocs is smaller than 2
   */
  public void setMaxBufferedDocs(int maxBufferedDocs) {
    synchronized(directory) {
      assureOpen();
      if (indexWriter != null) {
        indexWriter.setMaxBufferedDocs(maxBufferedDocs);
      }
      this.maxBufferedDocs = maxBufferedDocs;
    }
  }

  /**
   * @throws IOException
   * @see IndexModifier#setMaxBufferedDocs(int)
   */
  public int getMaxBufferedDocs() throws IOException {
    synchronized(directory) {
      assureOpen();
      createIndexWriter();
      return indexWriter.getMaxBufferedDocs();
    }
  }

  /**
   * Determines how often segment indices are merged by addDocument().  With
   * smaller values, less RAM is used while indexing, and searches on
   * unoptimized indices are faster, but indexing speed is slower.  With larger
   * values, more RAM is used during indexing, and while searches on unoptimized
   * indices are slower, indexing is faster.  Thus larger values (&gt; 10) are best
   * for batch index creation, and smaller values (&lt; 10) for indices that are
   * interactively maintained.
   * <p>This must never be less than 2.  The default value is 10.
   *
   * @see IndexWriter#setMergeFactor(int)
   * @throws IllegalStateException if the index is closed
   */
  public void setMergeFactor(int mergeFactor) {
    synchronized(directory) {
      assureOpen();
      if (indexWriter != null) {
        indexWriter.setMergeFactor(mergeFactor);
      }
      this.mergeFactor = mergeFactor;
    }
  }

  /**
   * @throws IOException
   * @see IndexModifier#setMergeFactor(int)
   */
  public int getMergeFactor() throws IOException {
    synchronized(directory) {
      assureOpen();
      createIndexWriter();
      return indexWriter.getMergeFactor();
    }
  }

  /**
   * Close this index, writing all pending changes to disk.
   *
   * @throws IllegalStateException if the index has been closed before already
   */
  public void close() throws IOException {
    synchronized(directory) {
      if (!open)
        throw new IllegalStateException("Index is closed already");
      if (indexWriter != null) {
        indexWriter.close();
        indexWriter = null;
      } else {
        indexReader.close();
        indexReader = null;
      }
      open = false;
    }
  }

  public String toString() {
    return "Index@" + directory;
  }

  /*
  // used as an example in the javadoc:
  public static void main(String[] args) throws IOException {
    Analyzer analyzer = new StandardAnalyzer();
    // create an index in /tmp/index, overwriting an existing one:
    IndexModifier indexModifier = new IndexModifier("/tmp/index", analyzer, true);
    Document doc = new Document();
    doc.add(new Fieldable("id", "1", Fieldable.Store.YES, Fieldable.Index.UN_TOKENIZED));
    doc.add(new Fieldable("body", "a simple test", Fieldable.Store.YES, Fieldable.Index.TOKENIZED));
    indexModifier.addDocument(doc);
    int deleted = indexModifier.delete(new Term("id", "1"));
    System.out.println("Deleted " + deleted + " document");
    indexModifier.flush();
    System.out.println(indexModifier.docCount() + " docs in index");
    indexModifier.close();
  }*/
  
}
