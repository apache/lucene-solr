package org.apache.lucene.ant;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Vector;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.document.DateField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;

/**
 * Builds a Lucene index from a fileset.
 *
 * @author     Erik Hatcher
 */
public class IndexTask extends Task {
    /**
     *  file list
     */
    private Vector filesets = new Vector();

    /**
     *  overwrite index?
     */
    private boolean overwrite = false;

    /**
     *  index path
     */
    private File indexPath;

    /**
     *  document handler classname
     */
    private String handlerClassName =
            "org.apache.lucene.ant.FileExtensionDocumentHandler";

    /**
     *  document handler instance
     */
    private DocumentHandler handler;

    /**
     *  Lucene merge factor
     */
    private int mergeFactor = 20;


    /**
     *  Specifies the directory where the index will be stored
     *
     * @param  indexPath  The new index value
     */
    public void setIndex(File indexPath) {
        this.indexPath = indexPath;
    }

    /**
     *  Sets the mergeFactor attribute of the IndexTask object
     *
     *@param  mergeFactor  The new mergeFactor value
     */
    public void setMergeFactor(int mergeFactor) {
        this.mergeFactor = mergeFactor;
    }


    /**
     * If true, index will be overwritten.
     *
     * @param  overwrite  The new overwrite value
     */
    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }


    /**
     * Classname of document handler.
     *
     * @param  classname  The new documentHandler value
     */
    public void setDocumentHandler(String classname) {
        handlerClassName = classname;
    }


    /**
     *  Adds a set of files.
     *
     * @param  set  FileSet to be added
     */
    public void addFileset(FileSet set) {
        filesets.addElement(set);
    }


    /**
     *  Begins the indexing
     *
     * @exception  BuildException  If an error occurs indexing the
     *      fileset
     * @todo add classpath handling so handler does not
     *       have to be in system classpath
     */
    public void execute() throws BuildException {
        try {
            Class clazz = Class.forName(handlerClassName);
            handler = (DocumentHandler) clazz.newInstance();
        }
        catch (ClassNotFoundException cnfe) {
            throw new BuildException(cnfe);
        }
        catch (InstantiationException ie) {
            throw new BuildException(ie);
        }
        catch (IllegalAccessException iae) {
            throw new BuildException(iae);
        }

        try {
            indexDocs();
        }
        catch (IOException e) {
            throw new BuildException(e);
        }
    }


    /**
     *  index the fileset
     *
     * @exception  IOException  Description of Exception
     * @todo refactor - definitely lots of room for improvement here
     */
    private void indexDocs() throws IOException {
        Date start = new Date();

        boolean create = overwrite;
        // If the index directory doesn't exist,
        // create it and force create mode
        if (indexPath.mkdirs() && !overwrite) {
            create = true;
        }

        Searcher searcher = null;
        Analyzer analyzer = new StopAnalyzer();
        boolean checkLastModified = false;
        if (!create) {
            try {
                searcher = new IndexSearcher(indexPath.getAbsolutePath());
                checkLastModified = true;
            }
            catch (IOException ioe) {
                log("IOException: " + ioe.getMessage());
                // Empty - ignore, which indicates to index all
                // documents
            }
        }

        log("checkLastModified = " + checkLastModified);

        IndexWriter writer =
                       new IndexWriter(indexPath, analyzer, create);
        int totalFiles = 0;
        int totalIndexed = 0;
        int totalIgnored = 0;
        try {
            writer.mergeFactor = mergeFactor;

            for (int i = 0; i < filesets.size(); i++) {
                FileSet fs = (FileSet) filesets.elementAt(i);
                if (fs != null) {
                    DirectoryScanner ds =
                                   fs.getDirectoryScanner(getProject());
                    String[] dsfiles = ds.getIncludedFiles();
                    File baseDir = ds.getBasedir();

                    for (int j = 0; j < dsfiles.length; j++) {
                        File file = new File(baseDir, dsfiles[j]);
                        totalFiles++;

                        if (!file.exists() || !file.canRead()) {
                            throw new BuildException("File \"" +
                        file.getAbsolutePath()
                        + "\" does not exist or is not readable.");
                        }

                        boolean indexIt = true;

                        if (checkLastModified) {
                            Hits hits = null;
                            Term pathTerm =
                                  new Term("path", file.getPath());
                            TermQuery query =
                                           new TermQuery(pathTerm);
                            hits = searcher.search(query);

                            // if document is found, compare the
                            // indexed last modified time with the
                            // current file
                            // - don't index if up to date
                            if (hits.length() > 0) {
                                Document doc = hits.doc(0);
                                String indexModified =
                                               doc.get("modified");
                                if (indexModified != null) {
                                    if (DateField.stringToTime(indexModified)
                                             == file.lastModified()) {
                                        indexIt = false;
                                    }
                                }
                            }
                        }

                        if (indexIt) {
                            try {
                                log("Indexing " + file.getPath(),
                                    Project.MSG_VERBOSE);
                                Document doc =
                                         handler.getDocument(file);

                                if (doc == null) {
                                    totalIgnored++;
                                }
                                else {
                                    // Add the path of the file as a field named "path".  Use a Text field, so
                                    // that the index stores the path, and so that the path is searchable
                                    doc.add(Field.Keyword("path", file.getPath()));

                                    // Add the last modified date of the file a field named "modified".  Use a
                                    // Keyword field, so that it's searchable, but so that no attempt is made
                                    // to tokenize the field into words.
                                    doc.add(Field.Keyword("modified",
                                            DateField.timeToString(file.lastModified())));

                                    writer.addDocument(doc);
                                    totalIndexed++;
                                }
                            }
                            catch (DocumentHandlerException e) {
                                throw new BuildException(e);
                            }
                        }
                    }
                    // for j
                }
                // if (fs != null)
            }
            // for i

            writer.optimize();
        }
        //try
        finally {
            // always make sure everything gets closed,
            // no matter how we exit.
            writer.close();
            if (searcher != null) {
                searcher.close();
            }
        }

        Date end = new Date();

        log(totalIndexed + " out of " + totalFiles + " indexed (" +
                totalIgnored + " ignored) in " + (end.getTime() - start.getTime()) +
                " milliseconds");
    }
}

