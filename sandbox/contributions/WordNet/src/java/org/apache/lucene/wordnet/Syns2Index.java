package org.apache.lucene.wordnet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Document;

import java.io.FileInputStream;
import java.io.DataInputStream;
import java.io.PrintStream;
import java.io.File;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Convert the prolog file wn_s.pl from the wordnet prolog download
 * into a Lucene index suitable for looking up synonyms.
 * The index is named 'syn_index' and has fields named "word"
 * and "syn".
 * <p>
 * The source word (such as 'big') can be looked up in the
 * "word" field, and if present there will be fields named "syn"
 * for every synonym.
 * </p>
 * <p>
 * While the wordnet file distinguishes groups of synonyms with
 * related meanings we don't do that here.
 * </p>
 * <p>
 * By default, with no args, we expect the prolog
 * file to be at 'c:/proj/wordnet/prolog/wn_s.pl' and will
 * write to an index named 'syn_index' in the current dir.
 * See constants at the bottom of this file to change these.
 * </p>
 * See also:
 * <br/>
 * http://www.cogsci.princeton.edu/~wn/
 * <br/>
 * http://www.tropo.com/techno/java/lucene/wordnet.html
 *
 * @author Dave Spencer, dave@lumos.com
 */
public class Syns2Index
{
    private static final Analyzer ana = new StandardAnalyzer();

    /**
     * Takes optional arg of prolog file name.
     */
    public static void main(String[] args)
        throws Throwable
    {
        // get command line arguments
        String prologFilename = null;
        String indexDir = null;
        if (args.length == 2)
        {
            prologFilename = args[0];
            indexDir = args[1];
        }
        else
        {
            usage();
            System.exit(1);
        }

        // ensure that the prolog file is readable
        if (! (new File(prologFilename)).canRead())
        {
            System.err.println("Error: cannot read Prolog file: " + prologFilename);
            System.exit(1);
        }
        // exit if the target index directory already exists
        if ((new File(indexDir)).isDirectory())
        {
            System.err.println("Error: index directory already exists: " + indexDir);
            System.err.println("Please specify a name of a non-existent directory");
            System.exit(1);
        }

        System.out.println("Opening Prolog file " + prologFilename);
        final FileInputStream fis = new FileInputStream(prologFilename);
        final DataInputStream dis = new DataInputStream(fis);
        String line;

        // maps a word to all the "groups" it's in
        final Map word2Nums = new HashMap();
        // maps a group to all the words in it
        final Map num2Words = new HashMap();
        // number of rejected words
        int ndecent = 0;

        // status output
        int mod = 1;
        int row = 1;
        // parse prolog file
        while ((line = dis.readLine()) != null)
        {
            String oline = line;

            // occasional progress
            if ((++row) % mod == 0)
            {
                mod *= 2;
                System.out.println("" + row + " " + line + " " + word2Nums.size()
                    + " " + num2Words.size() + " ndecent=" + ndecent);
            }

            // syntax check
            if (! line.startsWith("s("))
            {
                System.err.println("OUCH: " + line);
                System.exit(1);
            }

            // parse line
            line = line.substring(2);
            int comma = line.indexOf(',');
            String num = line.substring(0, comma);
            int q1 = line.indexOf('\'');
            line = line.substring(q1 + 1);
            int q2 = line.indexOf('\'');
            String word = line.substring(0, q2).toLowerCase();

            // make sure is a normal word
            if (! isDecent(word))
            {
                ndecent++;
                continue; // don't store words w/ spaces
            }

            // 1/2: word2Nums map
            // append to entry or add new one
            List lis =(List) word2Nums.get(word);
            if (lis == null)
            {
                lis = new LinkedList();
                lis.add(num);
                word2Nums.put(word, lis);
            }
            else
                lis.add(num);

            // 2/2: num2Words map
            lis = (List) num2Words.get(num);
            if (lis == null)
            {
                lis = new LinkedList();
                lis.add(word);
                num2Words.put(num, lis);
            }
            else
                lis.add(word);
        }

        // close the streams
        fis.close();
        dis.close();

        // create the index
        index(indexDir, word2Nums, num2Words);
    }

    /**
     * Checks to see if a word contains only alphabetic characters by
     * checking it one character at a time.
     *
     * @param s string to check
     * @return <code>true</code> if the string is decent
     */
    private static boolean isDecent(String s)
    {
        int len = s.length();
        for (int i = 0; i < len; i++)
        {
            if (!Character.isLetter(s.charAt(i)))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Forms a Lucene index based on the 2 maps.
     *
     * @param indexDir the direcotry where the index should be created
     * @param word2Nums
     * @param num2Words
     */
    private static void index(String indexDir, Map word2Nums, Map num2Words)
        throws Throwable
    {
        int row = 0;
        int mod = 1;

        // override the specific index if it already exists
        IndexWriter writer = new IndexWriter(indexDir, ana, true);
        Iterator i1 = word2Nums.keySet().iterator();
        while (i1.hasNext()) // for each word
        {
            String g = (String) i1.next();
            Document doc = new Document();

            int n = index(word2Nums, num2Words, g, doc);
            if (n > 0)
            {
                doc.add(Field.Keyword("word", g));
                if ((++row % mod) == 0)
                {
                    System.out.println("row=" + row + " doc= " + doc);
                    mod *= 2;
                }
                writer.addDocument(doc);
            } // else degenerate
        }
        writer.optimize();
        writer.close();
    }

    /**
     * Given the 2 maps fills a document for 1 word.
     *
     * @param
     * @param
     * @param
     * @param
     * @return
     */
    private static int index(Map word2Nums, Map num2Words, String g, Document doc)
        throws Throwable
    {
        List keys = (List) word2Nums.get(g); // get list of key#'s
        Iterator i2 = keys.iterator();

        Set already = new TreeSet(); // keep them sorted

        // pass 1: fill up 'already' with all words
        while (i2.hasNext()) // for each key#
        {
            already.addAll((List) num2Words.get(i2.next())); // get list of words
        }
        int num = 0;
        already.remove(g); // of course a word is it's own syn
        Iterator it = already.iterator();
        while (it.hasNext())
        {
            String cur = (String) it.next();
            // don't store things like 'pit bull' -> 'american pit bull'
            if (!isDecent(cur))
            {
                continue;
            }
            num++;
            doc.add(Field.UnIndexed("syn" , cur));
        }
        return num;
    }

    private static void usage()
    {
        System.out.println("\n\n" +
            "java org.apache.lucene.wordnet.Syn2Index <prolog file> <index dir>\n\n");
    }
}
