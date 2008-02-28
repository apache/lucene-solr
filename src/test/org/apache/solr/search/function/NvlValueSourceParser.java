package org.apache.solr.search.function;

import org.apache.lucene.queryParser.ParseException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.SimpleFloatFunction;
import org.apache.solr.search.function.ValueSource;

/**
 * A sample ValueSourceParser for testing. Approximates the oracle NVL function,
 * letting you substitude a value when a "null" is encountered. In this case,
 * null is approximated by a float value, since ValueSource always returns a
 * float, even if the field is undefined for a document.
 * 
 * Initialization parameters:
 *  - nvlFloatValue: float value to consider as "NULL" when seen in a field. defaults to 0.0f.
 *  
 * Example:
 *   nvl(vs,2)   will return 2 if the vs is NULL (as defined by nvlFloatValue above) or the doc value otherwise
 * 
 */
public class NvlValueSourceParser extends ValueSourceParser {
    
    /**
     * Value to consider "null" when found in a ValueSource Defaults to 0.0
     */
    private float nvlFloatValue = 0.0f;

    public ValueSource parse(FunctionQParser fp) throws ParseException {
	ValueSource source = fp.parseValueSource();
	final float nvl = fp.parseFloat();

	return new SimpleFloatFunction(source) {
	    protected String name() {
		return "nvl";
	    }

	    protected float func(int doc, DocValues vals) {
		float v = vals.floatVal(doc);
		if (v == nvlFloatValue) {
		    return nvl;
		} else {
		    return v;
		}
	    }
	};
    }

    public void init(NamedList args) {
	/* initialize the value to consider as null */
	Float nvlFloatValueArg = (Float) args.get("nvlFloatValue");
	if (nvlFloatValueArg != null) {
	    this.nvlFloatValue = nvlFloatValueArg;
	}
    }
}