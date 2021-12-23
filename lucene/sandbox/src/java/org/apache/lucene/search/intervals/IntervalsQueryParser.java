/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.lucene.search.intervals;

/**
 *
 * @author John Murphy
 * @email john.murphy@elastic.co
 */




import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.index.Term;

/**
 * Created by murphy on 8/14/18.
 */
public class IntervalsQueryParser {
  private final objectHolder objectHolder;
  protected final Pattern quotes = Pattern.compile("\"[^\"(/]*\"");
  protected final Pattern parenthesis = Pattern.compile("\\([^)(]*\\)");
  protected final Pattern within = Pattern.compile("/[0-9]*");
  protected final Pattern wildcard = Pattern.compile("\\*");
  protected final Pattern space = Pattern.compile(("[\\s]"));

  public IntervalsQueryParser() {
    this.objectHolder = new objectHolder();
  }
  public IntervalQuery getQuery(String fieldName, String query) throws Exception {
    String code = makeQuery( fieldName, query.replaceAll("\\)\\(",") ("));
    IntervalQuery iq = new IntervalQuery(fieldName, objectHolder.getIntervalsSourceFromCode(code));
    System.out.println(iq.toString());
    return iq;
  }

  String makeQuery( String fieldName, String query) throws Exception {
    if(objectHolder.isCode(query))
      return query;
    while(true){
      Matcher m = quotes.matcher(query);
      if(!m.find())
        break;
      String sub = query.substring(m.start()+1,m.end()-1);
      String replacement = processString(fieldName, sub);
      query = m.replaceFirst(replacement);
    }
    while(true){
      Matcher m = parenthesis.matcher(query);
      if(!m.find())
        break;
      String sub = query.substring(m.start()+1,m.end()-1);
      String replacement = makeQuery( fieldName, sub);
      query = m.replaceFirst(replacement);
      query = makeQuery(fieldName, query);
    }
    while(true){
      Matcher m = within.matcher(query);
      if(!m.find())
        break;
      String srange = query.substring(m.start()+1, m.end());
      int range = Integer.parseInt(srange);
      String first = query.substring(0,m.start()-1);
      String second = query.substring(m.end()+1, query.length());
      String firsteplacement = makeQuery( fieldName, first);
      String secondreplacement = makeQuery( fieldName, second);
      IntervalsSource sqFirst = objectHolder.getIntervalsSourceFromCode(firsteplacement);
      IntervalsSource sqSecond = objectHolder.getIntervalsSourceFromCode(secondreplacement);
      IntervalsSource snq = Intervals.unordered(sqFirst, sqSecond);
      IntervalsSource irange = Intervals.maxwidth(range, snq);
      String code = objectHolder.addIntervalsSource(fieldName, irange);
      query = code;
      query = makeQuery(fieldName, query);
      }
          //String
      query = query.trim();
      String[] subqueries = space.split(query);
      if(subqueries.length==1){
        query = processString( fieldName, query);
      }
      if(subqueries.length > 1){
        IntervalsSource[] codes = new IntervalsSource[subqueries.length];
        for(int i=0;i<subqueries.length;i++){
          codes[i] = objectHolder.getIntervalsSourceFromCode(processString( fieldName, subqueries[i]));
      }
      IntervalsSource soq = Intervals.or(codes);
      String ret = objectHolder.addIntervalsSource(fieldName, soq);
      query = makeQuery( fieldName, ret);
    }
    return query;
  }


  String processString( String fieldName, String query) throws Exception {
    if(objectHolder.isCode(query))
      return query;
    String[] toks = query.split("[\\s]");
    for(int i=0;i<toks.length;i++) {
      String ganal = toks[i];
      if (ganal.contains(" ")) {
        String rganal = ganal.replace(" ", ".");
        toks[i] = toks[i].replaceAll(rganal, ganal);
        toks[i] = processString(fieldName,toks[i]);
      }
    }
    if(toks.length < 1)
      throw new Exception("No tokens in stream " + query);
    if(toks.length == 1) {
      if(objectHolder.isCode(toks[0]))
        return toks[0];
      if(query.contains("*"))
        return objectHolder.addIntervalsSource(fieldName, Intervals.wildcard( query));
      return objectHolder.addIntervalsSource(fieldName, Intervals.term(toks[0]));
    }
    else {
      IntervalsSource[] sq = new IntervalsSource[toks.length];
      for(int i=0;i<toks.length;i++){
        if(objectHolder.isCode(toks[i]))
          sq[i] = objectHolder.getIntervalsSourceFromCode(toks[i]);
        else{
          sq[i] = objectHolder.getIntervalsSourceFromCode(processString(fieldName, toks[i]));
        }                    
      }
      IntervalsSource snq = Intervals.phrase(sq);
      return objectHolder.addIntervalsSource(fieldName, snq);
    }

  }
  class objectHolder{
    private final HashMap<String, IntervalsSource> hashmap;
    private int count = 0;
    public objectHolder(){
      hashmap = new HashMap();
    }
    public String addIntervalsSource(String fieldName, IntervalsSource sq){
      if(sq.getClass().toString().equals("class org.apache.lucene.search.intervals.IntervalsSource")){
        Set<Term> st = new LinkedHashSet();
        sq.extractTerms(fieldName, st);
        if(st.isEmpty())
          return null;
      }
      String code = String.format("zz%012dzz", count);
      hashmap.put(code, sq);
      count++;
      return code;
    }
    public IntervalsSource getIntervalsSourceFromCode(String code){
      return hashmap.get(code);
    }
    public Boolean isCode(String code){
      return hashmap.containsKey(code);
    }
  }
}