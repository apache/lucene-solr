// This file was generated automatically by the Snowball to Java compiler

package org.tartarus.snowball.ext;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.Among;

/**
 * Generated class implementing code defined by a snowball script.
 */
public class LovinsStemmer extends SnowballProgram {

        private Among a_0[] = {
            new Among ( "d", -1, -1, "", this),
            new Among ( "f", -1, -1, "", this),
            new Among ( "ph", -1, -1, "", this),
            new Among ( "th", -1, -1, "", this),
            new Among ( "l", -1, -1, "", this),
            new Among ( "er", -1, -1, "", this),
            new Among ( "or", -1, -1, "", this),
            new Among ( "es", -1, -1, "", this),
            new Among ( "t", -1, -1, "", this)
        };

        private Among a_1[] = {
            new Among ( "s'", -1, 1, "r_A", this),
            new Among ( "a", -1, 1, "r_A", this),
            new Among ( "ia", 1, 1, "r_A", this),
            new Among ( "ata", 1, 1, "r_A", this),
            new Among ( "ic", -1, 1, "r_A", this),
            new Among ( "aic", 4, 1, "r_A", this),
            new Among ( "allic", 4, 1, "r_BB", this),
            new Among ( "aric", 4, 1, "r_A", this),
            new Among ( "atic", 4, 1, "r_B", this),
            new Among ( "itic", 4, 1, "r_H", this),
            new Among ( "antic", 4, 1, "r_C", this),
            new Among ( "istic", 4, 1, "r_A", this),
            new Among ( "alistic", 11, 1, "r_B", this),
            new Among ( "aristic", 11, 1, "r_A", this),
            new Among ( "ivistic", 11, 1, "r_A", this),
            new Among ( "ed", -1, 1, "r_E", this),
            new Among ( "anced", 15, 1, "r_B", this),
            new Among ( "enced", 15, 1, "r_A", this),
            new Among ( "ished", 15, 1, "r_A", this),
            new Among ( "ied", 15, 1, "r_A", this),
            new Among ( "ened", 15, 1, "r_E", this),
            new Among ( "ioned", 15, 1, "r_A", this),
            new Among ( "ated", 15, 1, "r_I", this),
            new Among ( "ented", 15, 1, "r_C", this),
            new Among ( "ized", 15, 1, "r_F", this),
            new Among ( "arized", 24, 1, "r_A", this),
            new Among ( "oid", -1, 1, "r_A", this),
            new Among ( "aroid", 26, 1, "r_A", this),
            new Among ( "hood", -1, 1, "r_A", this),
            new Among ( "ehood", 28, 1, "r_A", this),
            new Among ( "ihood", 28, 1, "r_A", this),
            new Among ( "elihood", 30, 1, "r_E", this),
            new Among ( "ward", -1, 1, "r_A", this),
            new Among ( "e", -1, 1, "r_A", this),
            new Among ( "ae", 33, 1, "r_A", this),
            new Among ( "ance", 33, 1, "r_B", this),
            new Among ( "icance", 35, 1, "r_A", this),
            new Among ( "ence", 33, 1, "r_A", this),
            new Among ( "ide", 33, 1, "r_L", this),
            new Among ( "icide", 38, 1, "r_A", this),
            new Among ( "otide", 38, 1, "r_A", this),
            new Among ( "age", 33, 1, "r_B", this),
            new Among ( "able", 33, 1, "r_A", this),
            new Among ( "atable", 42, 1, "r_A", this),
            new Among ( "izable", 42, 1, "r_E", this),
            new Among ( "arizable", 44, 1, "r_A", this),
            new Among ( "ible", 33, 1, "r_A", this),
            new Among ( "encible", 46, 1, "r_A", this),
            new Among ( "ene", 33, 1, "r_E", this),
            new Among ( "ine", 33, 1, "r_M", this),
            new Among ( "idine", 49, 1, "r_I", this),
            new Among ( "one", 33, 1, "r_R", this),
            new Among ( "ature", 33, 1, "r_E", this),
            new Among ( "eature", 52, 1, "r_Z", this),
            new Among ( "ese", 33, 1, "r_A", this),
            new Among ( "wise", 33, 1, "r_A", this),
            new Among ( "ate", 33, 1, "r_A", this),
            new Among ( "entiate", 56, 1, "r_A", this),
            new Among ( "inate", 56, 1, "r_A", this),
            new Among ( "ionate", 56, 1, "r_D", this),
            new Among ( "ite", 33, 1, "r_AA", this),
            new Among ( "ive", 33, 1, "r_A", this),
            new Among ( "ative", 61, 1, "r_A", this),
            new Among ( "ize", 33, 1, "r_F", this),
            new Among ( "alize", 63, 1, "r_A", this),
            new Among ( "icalize", 64, 1, "r_A", this),
            new Among ( "ialize", 64, 1, "r_A", this),
            new Among ( "entialize", 66, 1, "r_A", this),
            new Among ( "ionalize", 64, 1, "r_A", this),
            new Among ( "arize", 63, 1, "r_A", this),
            new Among ( "ing", -1, 1, "r_N", this),
            new Among ( "ancing", 70, 1, "r_B", this),
            new Among ( "encing", 70, 1, "r_A", this),
            new Among ( "aging", 70, 1, "r_B", this),
            new Among ( "ening", 70, 1, "r_E", this),
            new Among ( "ioning", 70, 1, "r_A", this),
            new Among ( "ating", 70, 1, "r_I", this),
            new Among ( "enting", 70, 1, "r_C", this),
            new Among ( "ying", 70, 1, "r_B", this),
            new Among ( "izing", 70, 1, "r_F", this),
            new Among ( "arizing", 79, 1, "r_A", this),
            new Among ( "ish", -1, 1, "r_C", this),
            new Among ( "yish", 81, 1, "r_A", this),
            new Among ( "i", -1, 1, "r_A", this),
            new Among ( "al", -1, 1, "r_BB", this),
            new Among ( "ical", 84, 1, "r_A", this),
            new Among ( "aical", 85, 1, "r_A", this),
            new Among ( "istical", 85, 1, "r_A", this),
            new Among ( "oidal", 84, 1, "r_A", this),
            new Among ( "eal", 84, 1, "r_Y", this),
            new Among ( "ial", 84, 1, "r_A", this),
            new Among ( "ancial", 90, 1, "r_A", this),
            new Among ( "arial", 90, 1, "r_A", this),
            new Among ( "ential", 90, 1, "r_A", this),
            new Among ( "ional", 84, 1, "r_A", this),
            new Among ( "ational", 94, 1, "r_B", this),
            new Among ( "izational", 95, 1, "r_A", this),
            new Among ( "ental", 84, 1, "r_A", this),
            new Among ( "ful", -1, 1, "r_A", this),
            new Among ( "eful", 98, 1, "r_A", this),
            new Among ( "iful", 98, 1, "r_A", this),
            new Among ( "yl", -1, 1, "r_R", this),
            new Among ( "ism", -1, 1, "r_B", this),
            new Among ( "icism", 102, 1, "r_A", this),
            new Among ( "oidism", 102, 1, "r_A", this),
            new Among ( "alism", 102, 1, "r_B", this),
            new Among ( "icalism", 105, 1, "r_A", this),
            new Among ( "ionalism", 105, 1, "r_A", this),
            new Among ( "inism", 102, 1, "r_J", this),
            new Among ( "ativism", 102, 1, "r_A", this),
            new Among ( "um", -1, 1, "r_U", this),
            new Among ( "ium", 110, 1, "r_A", this),
            new Among ( "ian", -1, 1, "r_A", this),
            new Among ( "ician", 112, 1, "r_A", this),
            new Among ( "en", -1, 1, "r_F", this),
            new Among ( "ogen", 114, 1, "r_A", this),
            new Among ( "on", -1, 1, "r_S", this),
            new Among ( "ion", 116, 1, "r_Q", this),
            new Among ( "ation", 117, 1, "r_B", this),
            new Among ( "ication", 118, 1, "r_G", this),
            new Among ( "entiation", 118, 1, "r_A", this),
            new Among ( "ination", 118, 1, "r_A", this),
            new Among ( "isation", 118, 1, "r_A", this),
            new Among ( "arisation", 122, 1, "r_A", this),
            new Among ( "entation", 118, 1, "r_A", this),
            new Among ( "ization", 118, 1, "r_F", this),
            new Among ( "arization", 125, 1, "r_A", this),
            new Among ( "action", 117, 1, "r_G", this),
            new Among ( "o", -1, 1, "r_A", this),
            new Among ( "ar", -1, 1, "r_X", this),
            new Among ( "ear", 129, 1, "r_Y", this),
            new Among ( "ier", -1, 1, "r_A", this),
            new Among ( "ariser", -1, 1, "r_A", this),
            new Among ( "izer", -1, 1, "r_F", this),
            new Among ( "arizer", 133, 1, "r_A", this),
            new Among ( "or", -1, 1, "r_T", this),
            new Among ( "ator", 135, 1, "r_A", this),
            new Among ( "s", -1, 1, "r_W", this),
            new Among ( "'s", 137, 1, "r_A", this),
            new Among ( "as", 137, 1, "r_B", this),
            new Among ( "ics", 137, 1, "r_A", this),
            new Among ( "istics", 140, 1, "r_A", this),
            new Among ( "es", 137, 1, "r_E", this),
            new Among ( "ances", 142, 1, "r_B", this),
            new Among ( "ences", 142, 1, "r_A", this),
            new Among ( "ides", 142, 1, "r_L", this),
            new Among ( "oides", 145, 1, "r_A", this),
            new Among ( "ages", 142, 1, "r_B", this),
            new Among ( "ies", 142, 1, "r_P", this),
            new Among ( "acies", 148, 1, "r_A", this),
            new Among ( "ancies", 148, 1, "r_A", this),
            new Among ( "encies", 148, 1, "r_A", this),
            new Among ( "aries", 148, 1, "r_A", this),
            new Among ( "ities", 148, 1, "r_A", this),
            new Among ( "alities", 153, 1, "r_A", this),
            new Among ( "ivities", 153, 1, "r_A", this),
            new Among ( "ines", 142, 1, "r_M", this),
            new Among ( "nesses", 142, 1, "r_A", this),
            new Among ( "ates", 142, 1, "r_A", this),
            new Among ( "atives", 142, 1, "r_A", this),
            new Among ( "ings", 137, 1, "r_N", this),
            new Among ( "is", 137, 1, "r_A", this),
            new Among ( "als", 137, 1, "r_BB", this),
            new Among ( "ials", 162, 1, "r_A", this),
            new Among ( "entials", 163, 1, "r_A", this),
            new Among ( "ionals", 162, 1, "r_A", this),
            new Among ( "isms", 137, 1, "r_B", this),
            new Among ( "ians", 137, 1, "r_A", this),
            new Among ( "icians", 167, 1, "r_A", this),
            new Among ( "ions", 137, 1, "r_B", this),
            new Among ( "ations", 169, 1, "r_B", this),
            new Among ( "arisations", 170, 1, "r_A", this),
            new Among ( "entations", 170, 1, "r_A", this),
            new Among ( "izations", 170, 1, "r_A", this),
            new Among ( "arizations", 173, 1, "r_A", this),
            new Among ( "ars", 137, 1, "r_O", this),
            new Among ( "iers", 137, 1, "r_A", this),
            new Among ( "izers", 137, 1, "r_F", this),
            new Among ( "ators", 137, 1, "r_A", this),
            new Among ( "less", 137, 1, "r_A", this),
            new Among ( "eless", 179, 1, "r_A", this),
            new Among ( "ness", 137, 1, "r_A", this),
            new Among ( "eness", 181, 1, "r_E", this),
            new Among ( "ableness", 182, 1, "r_A", this),
            new Among ( "eableness", 183, 1, "r_E", this),
            new Among ( "ibleness", 182, 1, "r_A", this),
            new Among ( "ateness", 182, 1, "r_A", this),
            new Among ( "iteness", 182, 1, "r_A", this),
            new Among ( "iveness", 182, 1, "r_A", this),
            new Among ( "ativeness", 188, 1, "r_A", this),
            new Among ( "ingness", 181, 1, "r_A", this),
            new Among ( "ishness", 181, 1, "r_A", this),
            new Among ( "iness", 181, 1, "r_A", this),
            new Among ( "ariness", 192, 1, "r_E", this),
            new Among ( "alness", 181, 1, "r_A", this),
            new Among ( "icalness", 194, 1, "r_A", this),
            new Among ( "antialness", 194, 1, "r_A", this),
            new Among ( "entialness", 194, 1, "r_A", this),
            new Among ( "ionalness", 194, 1, "r_A", this),
            new Among ( "fulness", 181, 1, "r_A", this),
            new Among ( "lessness", 181, 1, "r_A", this),
            new Among ( "ousness", 181, 1, "r_A", this),
            new Among ( "eousness", 201, 1, "r_A", this),
            new Among ( "iousness", 201, 1, "r_A", this),
            new Among ( "itousness", 201, 1, "r_A", this),
            new Among ( "entness", 181, 1, "r_A", this),
            new Among ( "ants", 137, 1, "r_B", this),
            new Among ( "ists", 137, 1, "r_A", this),
            new Among ( "icists", 207, 1, "r_A", this),
            new Among ( "us", 137, 1, "r_V", this),
            new Among ( "ous", 209, 1, "r_A", this),
            new Among ( "eous", 210, 1, "r_A", this),
            new Among ( "aceous", 211, 1, "r_A", this),
            new Among ( "antaneous", 211, 1, "r_A", this),
            new Among ( "ious", 210, 1, "r_A", this),
            new Among ( "acious", 214, 1, "r_B", this),
            new Among ( "itous", 210, 1, "r_A", this),
            new Among ( "ant", -1, 1, "r_B", this),
            new Among ( "icant", 217, 1, "r_A", this),
            new Among ( "ent", -1, 1, "r_C", this),
            new Among ( "ement", 219, 1, "r_A", this),
            new Among ( "izement", 220, 1, "r_A", this),
            new Among ( "ist", -1, 1, "r_A", this),
            new Among ( "icist", 222, 1, "r_A", this),
            new Among ( "alist", 222, 1, "r_A", this),
            new Among ( "icalist", 224, 1, "r_A", this),
            new Among ( "ialist", 224, 1, "r_A", this),
            new Among ( "ionist", 222, 1, "r_A", this),
            new Among ( "entist", 222, 1, "r_A", this),
            new Among ( "y", -1, 1, "r_B", this),
            new Among ( "acy", 229, 1, "r_A", this),
            new Among ( "ancy", 229, 1, "r_B", this),
            new Among ( "ency", 229, 1, "r_A", this),
            new Among ( "ly", 229, 1, "r_B", this),
            new Among ( "ealy", 233, 1, "r_Y", this),
            new Among ( "ably", 233, 1, "r_A", this),
            new Among ( "ibly", 233, 1, "r_A", this),
            new Among ( "edly", 233, 1, "r_E", this),
            new Among ( "iedly", 237, 1, "r_A", this),
            new Among ( "ely", 233, 1, "r_E", this),
            new Among ( "ately", 239, 1, "r_A", this),
            new Among ( "ively", 239, 1, "r_A", this),
            new Among ( "atively", 241, 1, "r_A", this),
            new Among ( "ingly", 233, 1, "r_B", this),
            new Among ( "atingly", 243, 1, "r_A", this),
            new Among ( "ily", 233, 1, "r_A", this),
            new Among ( "lily", 245, 1, "r_A", this),
            new Among ( "arily", 245, 1, "r_A", this),
            new Among ( "ally", 233, 1, "r_B", this),
            new Among ( "ically", 248, 1, "r_A", this),
            new Among ( "aically", 249, 1, "r_A", this),
            new Among ( "allically", 249, 1, "r_C", this),
            new Among ( "istically", 249, 1, "r_A", this),
            new Among ( "alistically", 252, 1, "r_B", this),
            new Among ( "oidally", 248, 1, "r_A", this),
            new Among ( "ially", 248, 1, "r_A", this),
            new Among ( "entially", 255, 1, "r_A", this),
            new Among ( "ionally", 248, 1, "r_A", this),
            new Among ( "ationally", 257, 1, "r_B", this),
            new Among ( "izationally", 258, 1, "r_B", this),
            new Among ( "entally", 248, 1, "r_A", this),
            new Among ( "fully", 233, 1, "r_A", this),
            new Among ( "efully", 261, 1, "r_A", this),
            new Among ( "ifully", 261, 1, "r_A", this),
            new Among ( "enly", 233, 1, "r_E", this),
            new Among ( "arly", 233, 1, "r_K", this),
            new Among ( "early", 265, 1, "r_Y", this),
            new Among ( "lessly", 233, 1, "r_A", this),
            new Among ( "ously", 233, 1, "r_A", this),
            new Among ( "eously", 268, 1, "r_A", this),
            new Among ( "iously", 268, 1, "r_A", this),
            new Among ( "ently", 233, 1, "r_A", this),
            new Among ( "ary", 229, 1, "r_F", this),
            new Among ( "ery", 229, 1, "r_E", this),
            new Among ( "icianry", 229, 1, "r_A", this),
            new Among ( "atory", 229, 1, "r_A", this),
            new Among ( "ity", 229, 1, "r_A", this),
            new Among ( "acity", 276, 1, "r_A", this),
            new Among ( "icity", 276, 1, "r_A", this),
            new Among ( "eity", 276, 1, "r_A", this),
            new Among ( "ality", 276, 1, "r_A", this),
            new Among ( "icality", 280, 1, "r_A", this),
            new Among ( "iality", 280, 1, "r_A", this),
            new Among ( "antiality", 282, 1, "r_A", this),
            new Among ( "entiality", 282, 1, "r_A", this),
            new Among ( "ionality", 280, 1, "r_A", this),
            new Among ( "elity", 276, 1, "r_A", this),
            new Among ( "ability", 276, 1, "r_A", this),
            new Among ( "izability", 287, 1, "r_A", this),
            new Among ( "arizability", 288, 1, "r_A", this),
            new Among ( "ibility", 276, 1, "r_A", this),
            new Among ( "inity", 276, 1, "r_CC", this),
            new Among ( "arity", 276, 1, "r_B", this),
            new Among ( "ivity", 276, 1, "r_A", this)
        };

        private Among a_2[] = {
            new Among ( "bb", -1, -1, "", this),
            new Among ( "dd", -1, -1, "", this),
            new Among ( "gg", -1, -1, "", this),
            new Among ( "ll", -1, -1, "", this),
            new Among ( "mm", -1, -1, "", this),
            new Among ( "nn", -1, -1, "", this),
            new Among ( "pp", -1, -1, "", this),
            new Among ( "rr", -1, -1, "", this),
            new Among ( "ss", -1, -1, "", this),
            new Among ( "tt", -1, -1, "", this)
        };

        private Among a_3[] = {
            new Among ( "uad", -1, 18, "", this),
            new Among ( "vad", -1, 19, "", this),
            new Among ( "cid", -1, 20, "", this),
            new Among ( "lid", -1, 21, "", this),
            new Among ( "erid", -1, 22, "", this),
            new Among ( "pand", -1, 23, "", this),
            new Among ( "end", -1, 24, "", this),
            new Among ( "ond", -1, 25, "", this),
            new Among ( "lud", -1, 26, "", this),
            new Among ( "rud", -1, 27, "", this),
            new Among ( "ul", -1, 9, "", this),
            new Among ( "her", -1, 28, "", this),
            new Among ( "metr", -1, 7, "", this),
            new Among ( "istr", -1, 6, "", this),
            new Among ( "urs", -1, 5, "", this),
            new Among ( "uct", -1, 2, "", this),
            new Among ( "et", -1, 32, "", this),
            new Among ( "mit", -1, 29, "", this),
            new Among ( "ent", -1, 30, "", this),
            new Among ( "umpt", -1, 3, "", this),
            new Among ( "rpt", -1, 4, "", this),
            new Among ( "ert", -1, 31, "", this),
            new Among ( "yt", -1, 33, "", this),
            new Among ( "iev", -1, 1, "", this),
            new Among ( "olv", -1, 8, "", this),
            new Among ( "ax", -1, 14, "", this),
            new Among ( "ex", -1, 15, "", this),
            new Among ( "bex", 26, 10, "", this),
            new Among ( "dex", 26, 11, "", this),
            new Among ( "pex", 26, 12, "", this),
            new Among ( "tex", 26, 13, "", this),
            new Among ( "ix", -1, 16, "", this),
            new Among ( "lux", -1, 17, "", this),
            new Among ( "yz", -1, 34, "", this)
        };


        private void copy_from(LovinsStemmer other) {
            super.copy_from(other);
        }

        private boolean r_A() {
            // (, line 21
            // hop, line 21
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            return true;
        }

        private boolean r_B() {
            // (, line 22
            // hop, line 22
            {
                int c = cursor - 3;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            return true;
        }

        private boolean r_C() {
            // (, line 23
            // hop, line 23
            {
                int c = cursor - 4;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            return true;
        }

        private boolean r_D() {
            // (, line 24
            // hop, line 24
            {
                int c = cursor - 5;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            return true;
        }

        private boolean r_E() {
            int v_1;
            int v_2;
            // (, line 25
            // test, line 25
            v_1 = limit - cursor;
            // hop, line 25
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // not, line 25
            {
                v_2 = limit - cursor;
                lab0: do {
                    // literal, line 25
                    if (!(eq_s_b(1, "e")))
                    {
                        break lab0;
                    }
                    return false;
                } while (false);
                cursor = limit - v_2;
            }
            return true;
        }

        private boolean r_F() {
            int v_1;
            int v_2;
            // (, line 26
            // test, line 26
            v_1 = limit - cursor;
            // hop, line 26
            {
                int c = cursor - 3;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // not, line 26
            {
                v_2 = limit - cursor;
                lab0: do {
                    // literal, line 26
                    if (!(eq_s_b(1, "e")))
                    {
                        break lab0;
                    }
                    return false;
                } while (false);
                cursor = limit - v_2;
            }
            return true;
        }

        private boolean r_G() {
            int v_1;
            // (, line 27
            // test, line 27
            v_1 = limit - cursor;
            // hop, line 27
            {
                int c = cursor - 3;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // literal, line 27
            if (!(eq_s_b(1, "f")))
            {
                return false;
            }
            return true;
        }

        private boolean r_H() {
            int v_1;
            int v_2;
            // (, line 28
            // test, line 28
            v_1 = limit - cursor;
            // hop, line 28
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // or, line 28
            lab0: do {
                v_2 = limit - cursor;
                lab1: do {
                    // literal, line 28
                    if (!(eq_s_b(1, "t")))
                    {
                        break lab1;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                // literal, line 28
                if (!(eq_s_b(2, "ll")))
                {
                    return false;
                }
            } while (false);
            return true;
        }

        private boolean r_I() {
            int v_1;
            int v_2;
            int v_3;
            // (, line 29
            // test, line 29
            v_1 = limit - cursor;
            // hop, line 29
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // not, line 29
            {
                v_2 = limit - cursor;
                lab0: do {
                    // literal, line 29
                    if (!(eq_s_b(1, "o")))
                    {
                        break lab0;
                    }
                    return false;
                } while (false);
                cursor = limit - v_2;
            }
            // not, line 29
            {
                v_3 = limit - cursor;
                lab1: do {
                    // literal, line 29
                    if (!(eq_s_b(1, "e")))
                    {
                        break lab1;
                    }
                    return false;
                } while (false);
                cursor = limit - v_3;
            }
            return true;
        }

        private boolean r_J() {
            int v_1;
            int v_2;
            int v_3;
            // (, line 30
            // test, line 30
            v_1 = limit - cursor;
            // hop, line 30
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // not, line 30
            {
                v_2 = limit - cursor;
                lab0: do {
                    // literal, line 30
                    if (!(eq_s_b(1, "a")))
                    {
                        break lab0;
                    }
                    return false;
                } while (false);
                cursor = limit - v_2;
            }
            // not, line 30
            {
                v_3 = limit - cursor;
                lab1: do {
                    // literal, line 30
                    if (!(eq_s_b(1, "e")))
                    {
                        break lab1;
                    }
                    return false;
                } while (false);
                cursor = limit - v_3;
            }
            return true;
        }

        private boolean r_K() {
            int v_1;
            int v_2;
            // (, line 31
            // test, line 31
            v_1 = limit - cursor;
            // hop, line 31
            {
                int c = cursor - 3;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // or, line 31
            lab0: do {
                v_2 = limit - cursor;
                lab1: do {
                    // literal, line 31
                    if (!(eq_s_b(1, "l")))
                    {
                        break lab1;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                lab2: do {
                    // literal, line 31
                    if (!(eq_s_b(1, "i")))
                    {
                        break lab2;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                // (, line 31
                // literal, line 31
                if (!(eq_s_b(1, "e")))
                {
                    return false;
                }
                // next, line 31
                if (cursor <= limit_backward)
                {
                    return false;
                }
                cursor--;
                // literal, line 31
                if (!(eq_s_b(1, "u")))
                {
                    return false;
                }
            } while (false);
            return true;
        }

        private boolean r_L() {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            // (, line 32
            // test, line 32
            v_1 = limit - cursor;
            // hop, line 32
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // not, line 32
            {
                v_2 = limit - cursor;
                lab0: do {
                    // literal, line 32
                    if (!(eq_s_b(1, "u")))
                    {
                        break lab0;
                    }
                    return false;
                } while (false);
                cursor = limit - v_2;
            }
            // not, line 32
            {
                v_3 = limit - cursor;
                lab1: do {
                    // literal, line 32
                    if (!(eq_s_b(1, "x")))
                    {
                        break lab1;
                    }
                    return false;
                } while (false);
                cursor = limit - v_3;
            }
            // not, line 32
            {
                v_4 = limit - cursor;
                lab2: do {
                    // (, line 32
                    // literal, line 32
                    if (!(eq_s_b(1, "s")))
                    {
                        break lab2;
                    }
                    // not, line 32
                    {
                        v_5 = limit - cursor;
                        lab3: do {
                            // literal, line 32
                            if (!(eq_s_b(1, "o")))
                            {
                                break lab3;
                            }
                            break lab2;
                        } while (false);
                        cursor = limit - v_5;
                    }
                    return false;
                } while (false);
                cursor = limit - v_4;
            }
            return true;
        }

        private boolean r_M() {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            // (, line 33
            // test, line 33
            v_1 = limit - cursor;
            // hop, line 33
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // not, line 33
            {
                v_2 = limit - cursor;
                lab0: do {
                    // literal, line 33
                    if (!(eq_s_b(1, "a")))
                    {
                        break lab0;
                    }
                    return false;
                } while (false);
                cursor = limit - v_2;
            }
            // not, line 33
            {
                v_3 = limit - cursor;
                lab1: do {
                    // literal, line 33
                    if (!(eq_s_b(1, "c")))
                    {
                        break lab1;
                    }
                    return false;
                } while (false);
                cursor = limit - v_3;
            }
            // not, line 33
            {
                v_4 = limit - cursor;
                lab2: do {
                    // literal, line 33
                    if (!(eq_s_b(1, "e")))
                    {
                        break lab2;
                    }
                    return false;
                } while (false);
                cursor = limit - v_4;
            }
            // not, line 33
            {
                v_5 = limit - cursor;
                lab3: do {
                    // literal, line 33
                    if (!(eq_s_b(1, "m")))
                    {
                        break lab3;
                    }
                    return false;
                } while (false);
                cursor = limit - v_5;
            }
            return true;
        }

        private boolean r_N() {
            int v_1;
            int v_2;
            int v_3;
            // (, line 34
            // test, line 34
            v_1 = limit - cursor;
            // hop, line 34
            {
                int c = cursor - 3;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // (, line 34
            // hop, line 34
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            // or, line 34
            lab0: do {
                v_2 = limit - cursor;
                lab1: do {
                    // not, line 34
                    {
                        v_3 = limit - cursor;
                        lab2: do {
                            // literal, line 34
                            if (!(eq_s_b(1, "s")))
                            {
                                break lab2;
                            }
                            break lab1;
                        } while (false);
                        cursor = limit - v_3;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                // hop, line 34
                {
                    int c = cursor - 2;
                    if (limit_backward > c || c > limit)
                    {
                        return false;
                    }
                    cursor = c;
                }
            } while (false);
            return true;
        }

        private boolean r_O() {
            int v_1;
            int v_2;
            // (, line 35
            // test, line 35
            v_1 = limit - cursor;
            // hop, line 35
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // or, line 35
            lab0: do {
                v_2 = limit - cursor;
                lab1: do {
                    // literal, line 35
                    if (!(eq_s_b(1, "l")))
                    {
                        break lab1;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                // literal, line 35
                if (!(eq_s_b(1, "i")))
                {
                    return false;
                }
            } while (false);
            return true;
        }

        private boolean r_P() {
            int v_1;
            int v_2;
            // (, line 36
            // test, line 36
            v_1 = limit - cursor;
            // hop, line 36
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // not, line 36
            {
                v_2 = limit - cursor;
                lab0: do {
                    // literal, line 36
                    if (!(eq_s_b(1, "c")))
                    {
                        break lab0;
                    }
                    return false;
                } while (false);
                cursor = limit - v_2;
            }
            return true;
        }

        private boolean r_Q() {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            // (, line 37
            // test, line 37
            v_1 = limit - cursor;
            // hop, line 37
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // test, line 37
            v_2 = limit - cursor;
            // hop, line 37
            {
                int c = cursor - 3;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_2;
            // not, line 37
            {
                v_3 = limit - cursor;
                lab0: do {
                    // literal, line 37
                    if (!(eq_s_b(1, "l")))
                    {
                        break lab0;
                    }
                    return false;
                } while (false);
                cursor = limit - v_3;
            }
            // not, line 37
            {
                v_4 = limit - cursor;
                lab1: do {
                    // literal, line 37
                    if (!(eq_s_b(1, "n")))
                    {
                        break lab1;
                    }
                    return false;
                } while (false);
                cursor = limit - v_4;
            }
            return true;
        }

        private boolean r_R() {
            int v_1;
            int v_2;
            // (, line 38
            // test, line 38
            v_1 = limit - cursor;
            // hop, line 38
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // or, line 38
            lab0: do {
                v_2 = limit - cursor;
                lab1: do {
                    // literal, line 38
                    if (!(eq_s_b(1, "n")))
                    {
                        break lab1;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                // literal, line 38
                if (!(eq_s_b(1, "r")))
                {
                    return false;
                }
            } while (false);
            return true;
        }

        private boolean r_S() {
            int v_1;
            int v_2;
            int v_3;
            // (, line 39
            // test, line 39
            v_1 = limit - cursor;
            // hop, line 39
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // or, line 39
            lab0: do {
                v_2 = limit - cursor;
                lab1: do {
                    // literal, line 39
                    if (!(eq_s_b(2, "dr")))
                    {
                        break lab1;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                // (, line 39
                // literal, line 39
                if (!(eq_s_b(1, "t")))
                {
                    return false;
                }
                // not, line 39
                {
                    v_3 = limit - cursor;
                    lab2: do {
                        // literal, line 39
                        if (!(eq_s_b(1, "t")))
                        {
                            break lab2;
                        }
                        return false;
                    } while (false);
                    cursor = limit - v_3;
                }
            } while (false);
            return true;
        }

        private boolean r_T() {
            int v_1;
            int v_2;
            int v_3;
            // (, line 40
            // test, line 40
            v_1 = limit - cursor;
            // hop, line 40
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // or, line 40
            lab0: do {
                v_2 = limit - cursor;
                lab1: do {
                    // literal, line 40
                    if (!(eq_s_b(1, "s")))
                    {
                        break lab1;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                // (, line 40
                // literal, line 40
                if (!(eq_s_b(1, "t")))
                {
                    return false;
                }
                // not, line 40
                {
                    v_3 = limit - cursor;
                    lab2: do {
                        // literal, line 40
                        if (!(eq_s_b(1, "o")))
                        {
                            break lab2;
                        }
                        return false;
                    } while (false);
                    cursor = limit - v_3;
                }
            } while (false);
            return true;
        }

        private boolean r_U() {
            int v_1;
            int v_2;
            // (, line 41
            // test, line 41
            v_1 = limit - cursor;
            // hop, line 41
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // or, line 41
            lab0: do {
                v_2 = limit - cursor;
                lab1: do {
                    // literal, line 41
                    if (!(eq_s_b(1, "l")))
                    {
                        break lab1;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                lab2: do {
                    // literal, line 41
                    if (!(eq_s_b(1, "m")))
                    {
                        break lab2;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                lab3: do {
                    // literal, line 41
                    if (!(eq_s_b(1, "n")))
                    {
                        break lab3;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                // literal, line 41
                if (!(eq_s_b(1, "r")))
                {
                    return false;
                }
            } while (false);
            return true;
        }

        private boolean r_V() {
            int v_1;
            // (, line 42
            // test, line 42
            v_1 = limit - cursor;
            // hop, line 42
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // literal, line 42
            if (!(eq_s_b(1, "c")))
            {
                return false;
            }
            return true;
        }

        private boolean r_W() {
            int v_1;
            int v_2;
            int v_3;
            // (, line 43
            // test, line 43
            v_1 = limit - cursor;
            // hop, line 43
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // not, line 43
            {
                v_2 = limit - cursor;
                lab0: do {
                    // literal, line 43
                    if (!(eq_s_b(1, "s")))
                    {
                        break lab0;
                    }
                    return false;
                } while (false);
                cursor = limit - v_2;
            }
            // not, line 43
            {
                v_3 = limit - cursor;
                lab1: do {
                    // literal, line 43
                    if (!(eq_s_b(1, "u")))
                    {
                        break lab1;
                    }
                    return false;
                } while (false);
                cursor = limit - v_3;
            }
            return true;
        }

        private boolean r_X() {
            int v_1;
            int v_2;
            // (, line 44
            // test, line 44
            v_1 = limit - cursor;
            // hop, line 44
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // or, line 44
            lab0: do {
                v_2 = limit - cursor;
                lab1: do {
                    // literal, line 44
                    if (!(eq_s_b(1, "l")))
                    {
                        break lab1;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                lab2: do {
                    // literal, line 44
                    if (!(eq_s_b(1, "i")))
                    {
                        break lab2;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_2;
                // (, line 44
                // literal, line 44
                if (!(eq_s_b(1, "e")))
                {
                    return false;
                }
                // next, line 44
                if (cursor <= limit_backward)
                {
                    return false;
                }
                cursor--;
                // literal, line 44
                if (!(eq_s_b(1, "u")))
                {
                    return false;
                }
            } while (false);
            return true;
        }

        private boolean r_Y() {
            int v_1;
            // (, line 45
            // test, line 45
            v_1 = limit - cursor;
            // hop, line 45
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // literal, line 45
            if (!(eq_s_b(2, "in")))
            {
                return false;
            }
            return true;
        }

        private boolean r_Z() {
            int v_1;
            int v_2;
            // (, line 46
            // test, line 46
            v_1 = limit - cursor;
            // hop, line 46
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // not, line 46
            {
                v_2 = limit - cursor;
                lab0: do {
                    // literal, line 46
                    if (!(eq_s_b(1, "f")))
                    {
                        break lab0;
                    }
                    return false;
                } while (false);
                cursor = limit - v_2;
            }
            return true;
        }

        private boolean r_AA() {
            int v_1;
            // (, line 47
            // test, line 47
            v_1 = limit - cursor;
            // hop, line 47
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // among, line 47
            if (find_among_b(a_0, 9) == 0)
            {
                return false;
            }
            return true;
        }

        private boolean r_BB() {
            int v_1;
            int v_2;
            int v_3;
            // (, line 49
            // test, line 49
            v_1 = limit - cursor;
            // hop, line 49
            {
                int c = cursor - 3;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // not, line 49
            {
                v_2 = limit - cursor;
                lab0: do {
                    // literal, line 49
                    if (!(eq_s_b(3, "met")))
                    {
                        break lab0;
                    }
                    return false;
                } while (false);
                cursor = limit - v_2;
            }
            // not, line 49
            {
                v_3 = limit - cursor;
                lab1: do {
                    // literal, line 49
                    if (!(eq_s_b(4, "ryst")))
                    {
                        break lab1;
                    }
                    return false;
                } while (false);
                cursor = limit - v_3;
            }
            return true;
        }

        private boolean r_CC() {
            int v_1;
            // (, line 50
            // test, line 50
            v_1 = limit - cursor;
            // hop, line 50
            {
                int c = cursor - 2;
                if (limit_backward > c || c > limit)
                {
                    return false;
                }
                cursor = c;
            }
            cursor = limit - v_1;
            // literal, line 50
            if (!(eq_s_b(1, "l")))
            {
                return false;
            }
            return true;
        }

        private boolean r_endings() {
            int among_var;
            // (, line 55
            // [, line 56
            ket = cursor;
            // substring, line 56
            among_var = find_among_b(a_1, 294);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 56
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 145
                    // delete, line 145
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_undouble() {
            int v_1;
            // (, line 151
            // test, line 152
            v_1 = limit - cursor;
            // substring, line 152
            if (find_among_b(a_2, 10) == 0)
            {
                return false;
            }
            cursor = limit - v_1;
            // [, line 154
            ket = cursor;
            // next, line 154
            if (cursor <= limit_backward)
            {
                return false;
            }
            cursor--;
            // ], line 154
            bra = cursor;
            // delete, line 154
            slice_del();
            return true;
        }

        private boolean r_respell() {
            int among_var;
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            int v_8;
            // (, line 159
            // [, line 160
            ket = cursor;
            // substring, line 160
            among_var = find_among_b(a_3, 34);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 160
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 161
                    // <-, line 161
                    slice_from("ief");
                    break;
                case 2:
                    // (, line 162
                    // <-, line 162
                    slice_from("uc");
                    break;
                case 3:
                    // (, line 163
                    // <-, line 163
                    slice_from("um");
                    break;
                case 4:
                    // (, line 164
                    // <-, line 164
                    slice_from("rb");
                    break;
                case 5:
                    // (, line 165
                    // <-, line 165
                    slice_from("ur");
                    break;
                case 6:
                    // (, line 166
                    // <-, line 166
                    slice_from("ister");
                    break;
                case 7:
                    // (, line 167
                    // <-, line 167
                    slice_from("meter");
                    break;
                case 8:
                    // (, line 168
                    // <-, line 168
                    slice_from("olut");
                    break;
                case 9:
                    // (, line 169
                    // not, line 169
                    {
                        v_1 = limit - cursor;
                        lab0: do {
                            // literal, line 169
                            if (!(eq_s_b(1, "a")))
                            {
                                break lab0;
                            }
                            return false;
                        } while (false);
                        cursor = limit - v_1;
                    }
                    // not, line 169
                    {
                        v_2 = limit - cursor;
                        lab1: do {
                            // literal, line 169
                            if (!(eq_s_b(1, "i")))
                            {
                                break lab1;
                            }
                            return false;
                        } while (false);
                        cursor = limit - v_2;
                    }
                    // not, line 169
                    {
                        v_3 = limit - cursor;
                        lab2: do {
                            // literal, line 169
                            if (!(eq_s_b(1, "o")))
                            {
                                break lab2;
                            }
                            return false;
                        } while (false);
                        cursor = limit - v_3;
                    }
                    // <-, line 169
                    slice_from("l");
                    break;
                case 10:
                    // (, line 170
                    // <-, line 170
                    slice_from("bic");
                    break;
                case 11:
                    // (, line 171
                    // <-, line 171
                    slice_from("dic");
                    break;
                case 12:
                    // (, line 172
                    // <-, line 172
                    slice_from("pic");
                    break;
                case 13:
                    // (, line 173
                    // <-, line 173
                    slice_from("tic");
                    break;
                case 14:
                    // (, line 174
                    // <-, line 174
                    slice_from("ac");
                    break;
                case 15:
                    // (, line 175
                    // <-, line 175
                    slice_from("ec");
                    break;
                case 16:
                    // (, line 176
                    // <-, line 176
                    slice_from("ic");
                    break;
                case 17:
                    // (, line 177
                    // <-, line 177
                    slice_from("luc");
                    break;
                case 18:
                    // (, line 178
                    // <-, line 178
                    slice_from("uas");
                    break;
                case 19:
                    // (, line 179
                    // <-, line 179
                    slice_from("vas");
                    break;
                case 20:
                    // (, line 180
                    // <-, line 180
                    slice_from("cis");
                    break;
                case 21:
                    // (, line 181
                    // <-, line 181
                    slice_from("lis");
                    break;
                case 22:
                    // (, line 182
                    // <-, line 182
                    slice_from("eris");
                    break;
                case 23:
                    // (, line 183
                    // <-, line 183
                    slice_from("pans");
                    break;
                case 24:
                    // (, line 184
                    // not, line 184
                    {
                        v_4 = limit - cursor;
                        lab3: do {
                            // literal, line 184
                            if (!(eq_s_b(1, "s")))
                            {
                                break lab3;
                            }
                            return false;
                        } while (false);
                        cursor = limit - v_4;
                    }
                    // <-, line 184
                    slice_from("ens");
                    break;
                case 25:
                    // (, line 185
                    // <-, line 185
                    slice_from("ons");
                    break;
                case 26:
                    // (, line 186
                    // <-, line 186
                    slice_from("lus");
                    break;
                case 27:
                    // (, line 187
                    // <-, line 187
                    slice_from("rus");
                    break;
                case 28:
                    // (, line 188
                    // not, line 188
                    {
                        v_5 = limit - cursor;
                        lab4: do {
                            // literal, line 188
                            if (!(eq_s_b(1, "p")))
                            {
                                break lab4;
                            }
                            return false;
                        } while (false);
                        cursor = limit - v_5;
                    }
                    // not, line 188
                    {
                        v_6 = limit - cursor;
                        lab5: do {
                            // literal, line 188
                            if (!(eq_s_b(1, "t")))
                            {
                                break lab5;
                            }
                            return false;
                        } while (false);
                        cursor = limit - v_6;
                    }
                    // <-, line 188
                    slice_from("hes");
                    break;
                case 29:
                    // (, line 189
                    // <-, line 189
                    slice_from("mis");
                    break;
                case 30:
                    // (, line 190
                    // not, line 190
                    {
                        v_7 = limit - cursor;
                        lab6: do {
                            // literal, line 190
                            if (!(eq_s_b(1, "m")))
                            {
                                break lab6;
                            }
                            return false;
                        } while (false);
                        cursor = limit - v_7;
                    }
                    // <-, line 190
                    slice_from("ens");
                    break;
                case 31:
                    // (, line 192
                    // <-, line 192
                    slice_from("ers");
                    break;
                case 32:
                    // (, line 193
                    // not, line 193
                    {
                        v_8 = limit - cursor;
                        lab7: do {
                            // literal, line 193
                            if (!(eq_s_b(1, "n")))
                            {
                                break lab7;
                            }
                            return false;
                        } while (false);
                        cursor = limit - v_8;
                    }
                    // <-, line 193
                    slice_from("es");
                    break;
                case 33:
                    // (, line 194
                    // <-, line 194
                    slice_from("ys");
                    break;
                case 34:
                    // (, line 195
                    // <-, line 195
                    slice_from("ys");
                    break;
            }
            return true;
        }

        public boolean stem() {
            int v_1;
            int v_2;
            int v_3;
            // (, line 200
            // backwards, line 202
            limit_backward = cursor; cursor = limit;
            // (, line 202
            // do, line 203
            v_1 = limit - cursor;
            lab0: do {
                // call endings, line 203
                if (!r_endings())
                {
                    break lab0;
                }
            } while (false);
            cursor = limit - v_1;
            // do, line 204
            v_2 = limit - cursor;
            lab1: do {
                // call undouble, line 204
                if (!r_undouble())
                {
                    break lab1;
                }
            } while (false);
            cursor = limit - v_2;
            // do, line 205
            v_3 = limit - cursor;
            lab2: do {
                // call respell, line 205
                if (!r_respell())
                {
                    break lab2;
                }
            } while (false);
            cursor = limit - v_3;
            cursor = limit_backward;            return true;
        }

}

