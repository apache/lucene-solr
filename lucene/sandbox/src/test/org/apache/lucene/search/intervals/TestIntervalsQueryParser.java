/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.lucene.search.intervals;

import org.apache.lucene.util.LuceneTestCase;

/**
 *
 * @author murphy
 */
public class TestIntervalsQueryParser extends LuceneTestCase{
    public void testParsingstatic() throws Exception{
        IntervalsQueryParser iqp = new IntervalsQueryParser();
        assertEquals("or(a,b)", iqp.getQuery("content","a b").toString());
        assertEquals("or(a,b,c)", iqp.getQuery("content","(a b c)").toString());
        assertEquals("MAXWIDTH/5(UNORDERED(a,b))", iqp.getQuery("content","a /5 b").toString());
        assertEquals("or(MAXWIDTH/5(UNORDERED(a,b)),c)", iqp.getQuery("content","(a /5 b) c").toString());
        assertEquals("BLOCK(a,b)", iqp.getQuery("content","\"a b\"").toString());
        assertEquals("MAXWIDTH/3(UNORDERED(MAXWIDTH/3(UNORDERED(MAXWIDTH/3(UNORDERED(MAXWIDTH/3(UNORDERED(a,BLOCK(b,f,g))),c)),d)),e))", iqp.getQuery("content","((((a /3 \"b f g\") /3 c) /3 d) /3 e)").toString());
        assertEquals("MAXWIDTH/12(UNORDERED(or(can,could),MultiTerm(figur*)))", iqp.getQuery("content","(can could) /12 \"figur*\"").toString());
        assertEquals("MAXWIDTH/75(UNORDERED(MultiTerm(search*),MAXWIDTH/10(UNORDERED(MultiTerm(could*),BLOCK(MultiTerm(lat*),night)))))", iqp.getQuery("content","(search*) /75 (could* /10 \"lat* night\")").toString());
        assertEquals("BLOCK(MultiTerm(can*),MultiTerm(expect*))", iqp.getQuery("content","\"can* expect*\"").toString());
        assertEquals("BLOCK(or(can,might),MultiTerm(expect*))", iqp.getQuery("content","\"(can might) expect*\"").toString());
        assertEquals("\"(red green blue)(cluster* node* shard*)\"", iqp.getQuery("content","\"(red green blue)(cluster* node* shard*)\"").toString());
    }
}
