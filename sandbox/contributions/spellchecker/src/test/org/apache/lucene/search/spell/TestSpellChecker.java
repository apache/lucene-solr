package org.apache.lucene.search.spell;


import junit.framework.*;
import org.apache.lucene.search.spell.*;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.util.English;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import java.io.IOException;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Directory;
import java.io.File;


/**
 * Test case
 * @author Nicolas Maisonneuve
 */

public class TestSpellChecker
extends TestCase {
    private SpellChecker spellChecker;
    Directory userindex, spellindex;

    protected void setUp () throws Exception {
        super.setUp();

        //create a user index
        userindex=new RAMDirectory();
        IndexWriter writer=new IndexWriter(userindex, new SimpleAnalyzer(), true);

        for (int i=0; i<1000; i++) {
            Document doc=new Document();
            doc.add(Field.Text("field1", English.intToEnglish(i)));
            doc.add(Field.Text("field2", English.intToEnglish(i+1))); // + word thousand
            writer.addDocument(doc);
        }
        writer.close();

        // create the spellChecker
        File file=new File("d://test");
        spellindex=FSDirectory.getDirectory(file, true);
        spellChecker=new SpellChecker(spellindex);
    }


    public void testBuild () {
        try {
            IndexReader r=IndexReader.open(userindex);

            spellChecker.clearIndex();

            addwords(r, "field1");
            int num_field1=this.numdoc();

            addwords(r, "field2");
            int num_field2=this.numdoc();

            this.assertTrue(num_field2==num_field1+1);

            // test small word
            String[] l=spellChecker.suggestSimilar("fvie", 2);
            this.assertTrue(l[0].equals("five"));

            l=spellChecker.suggestSimilar("fiv", 2);
            this.assertTrue(l[0].equals("five"));

            l=spellChecker.suggestSimilar("ive", 2);
            this.assertTrue(l[0].equals("five"));

            l=spellChecker.suggestSimilar("fives", 2);
            this.assertTrue(l[0].equals("five"));

            l=spellChecker.suggestSimilar("fie", 2);
            this.assertTrue(l[0].equals("five"));

            l=spellChecker.suggestSimilar("fi", 2);
            this.assertEquals(0,l.length);

            // test restreint to a field
            l=spellChecker.suggestSimilar("tousand", 10, r, "field1", false);
            this.assertEquals(0,l.length); // there isn't the term thousand in the field field1

            l=spellChecker.suggestSimilar("tousand", 10, r, "field2", false);
            this.assertEquals(1,l.length); // there is the term thousand in the field field2
        }
        catch (IOException e) {
            e.printStackTrace();
            this.assertTrue(false);
        }

    }


    private void addwords (IndexReader r, String field) throws IOException {
        long time=System.currentTimeMillis();
        spellChecker.indexDictionnary(new LuceneDictionary(r, field));
        time=System.currentTimeMillis()-time;
        System.out.println("time to build "+field+": "+time);
    }


    private int numdoc () throws IOException {
        IndexReader rs=IndexReader.open(spellindex);
        int num=rs.numDocs();
        this.assertTrue(num!=0);
        System.out.println("num docs: "+num);
        rs.close();
        return num;
    }


    protected void tearDown () throws Exception {
        spellChecker=null;
        super.tearDown();
    }

}
