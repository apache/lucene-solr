// This file was generated automatically by the Snowball to Java compiler

package net.sf.snowball.ext;
import net.sf.snowball.SnowballProgram;
import net.sf.snowball.Among;

/**
 * Generated class implementing code defined by a snowball script.
 */
public class DanishStemmer extends SnowballProgram {

        private Among a_0[] = {
            new Among ( "hed", -1, 1, "", this),
            new Among ( "ethed", 0, 1, "", this),
            new Among ( "ered", -1, 1, "", this),
            new Among ( "e", -1, 1, "", this),
            new Among ( "erede", 3, 1, "", this),
            new Among ( "ende", 3, 1, "", this),
            new Among ( "erende", 5, 1, "", this),
            new Among ( "ene", 3, 1, "", this),
            new Among ( "erne", 3, 1, "", this),
            new Among ( "ere", 3, 1, "", this),
            new Among ( "en", -1, 1, "", this),
            new Among ( "heden", 10, 1, "", this),
            new Among ( "eren", 10, 1, "", this),
            new Among ( "er", -1, 1, "", this),
            new Among ( "heder", 13, 1, "", this),
            new Among ( "erer", 13, 1, "", this),
            new Among ( "s", -1, 2, "", this),
            new Among ( "heds", 16, 1, "", this),
            new Among ( "es", 16, 1, "", this),
            new Among ( "endes", 18, 1, "", this),
            new Among ( "erendes", 19, 1, "", this),
            new Among ( "enes", 18, 1, "", this),
            new Among ( "ernes", 18, 1, "", this),
            new Among ( "eres", 18, 1, "", this),
            new Among ( "ens", 16, 1, "", this),
            new Among ( "hedens", 24, 1, "", this),
            new Among ( "erens", 24, 1, "", this),
            new Among ( "ers", 16, 1, "", this),
            new Among ( "ets", 16, 1, "", this),
            new Among ( "erets", 28, 1, "", this),
            new Among ( "et", -1, 1, "", this),
            new Among ( "eret", 30, 1, "", this)
        };

        private Among a_1[] = {
            new Among ( "gd", -1, -1, "", this),
            new Among ( "dt", -1, -1, "", this),
            new Among ( "gt", -1, -1, "", this),
            new Among ( "kt", -1, -1, "", this)
        };

        private Among a_2[] = {
            new Among ( "ig", -1, 1, "", this),
            new Among ( "lig", 0, 1, "", this),
            new Among ( "elig", 1, 1, "", this),
            new Among ( "els", -1, 1, "", this),
            new Among ( "l\u00F8st", -1, 2, "", this)
        };

        private static final char g_v[] = {17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 0, 128 };

        private static final char g_s_ending[] = {239, 254, 42, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16 };

        private int I_p1;
        private StringBuffer S_ch = new StringBuffer();

        private void copy_from(DanishStemmer other) {
            I_p1 = other.I_p1;
            S_ch = other.S_ch;
            super.copy_from(other);
        }

        private boolean r_mark_regions() {
            int v_1;
            // (, line 29
            I_p1 = limit;
            // goto, line 33
            golab0: while(true)
            {
                v_1 = cursor;
                lab1: do {
                    if (!(in_grouping(g_v, 97, 248)))
                    {
                        break lab1;
                    }
                    cursor = v_1;
                    break golab0;
                } while (false);
                cursor = v_1;
                if (cursor >= limit)
                {
                    return false;
                }
                cursor++;
            }
            // gopast, line 33
            golab2: while(true)
            {
                lab3: do {
                    if (!(out_grouping(g_v, 97, 248)))
                    {
                        break lab3;
                    }
                    break golab2;
                } while (false);
                if (cursor >= limit)
                {
                    return false;
                }
                cursor++;
            }
            // setmark p1, line 33
            I_p1 = cursor;
            // try, line 34
            lab4: do {
                // (, line 34
                if (!(I_p1 < 3))
                {
                    break lab4;
                }
                I_p1 = 3;
            } while (false);
            return true;
        }

        private boolean r_main_suffix() {
            int among_var;
            int v_1;
            int v_2;
            // (, line 39
            // setlimit, line 40
            v_1 = limit - cursor;
            // tomark, line 40
            if (cursor < I_p1)
            {
                return false;
            }
            cursor = I_p1;
            v_2 = limit_backward;
            limit_backward = cursor;
            cursor = limit - v_1;
            // (, line 40
            // [, line 40
            ket = cursor;
            // substring, line 40
            among_var = find_among_b(a_0, 32);
            if (among_var == 0)
            {
                limit_backward = v_2;
                return false;
            }
            // ], line 40
            bra = cursor;
            limit_backward = v_2;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 47
                    // delete, line 47
                    slice_del();
                    break;
                case 2:
                    // (, line 49
                    if (!(in_grouping_b(g_s_ending, 97, 229)))
                    {
                        return false;
                    }
                    // delete, line 49
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_consonant_pair() {
            int v_1;
            int v_2;
            int v_3;
            // (, line 53
            // test, line 54
            v_1 = limit - cursor;
            // (, line 54
            // setlimit, line 55
            v_2 = limit - cursor;
            // tomark, line 55
            if (cursor < I_p1)
            {
                return false;
            }
            cursor = I_p1;
            v_3 = limit_backward;
            limit_backward = cursor;
            cursor = limit - v_2;
            // (, line 55
            // [, line 55
            ket = cursor;
            // substring, line 55
            if (find_among_b(a_1, 4) == 0)
            {
                limit_backward = v_3;
                return false;
            }
            // ], line 55
            bra = cursor;
            limit_backward = v_3;
            cursor = limit - v_1;
            // next, line 61
            if (cursor <= limit_backward)
            {
                return false;
            }
            cursor--;
            // ], line 61
            bra = cursor;
            // delete, line 61
            slice_del();
            return true;
        }

        private boolean r_other_suffix() {
            int among_var;
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            // (, line 64
            // do, line 65
            v_1 = limit - cursor;
            lab0: do {
                // (, line 65
                // [, line 65
                ket = cursor;
                // literal, line 65
                if (!(eq_s_b(2, "st")))
                {
                    break lab0;
                }
                // ], line 65
                bra = cursor;
                // literal, line 65
                if (!(eq_s_b(2, "ig")))
                {
                    break lab0;
                }
                // delete, line 65
                slice_del();
            } while (false);
            cursor = limit - v_1;
            // setlimit, line 66
            v_2 = limit - cursor;
            // tomark, line 66
            if (cursor < I_p1)
            {
                return false;
            }
            cursor = I_p1;
            v_3 = limit_backward;
            limit_backward = cursor;
            cursor = limit - v_2;
            // (, line 66
            // [, line 66
            ket = cursor;
            // substring, line 66
            among_var = find_among_b(a_2, 5);
            if (among_var == 0)
            {
                limit_backward = v_3;
                return false;
            }
            // ], line 66
            bra = cursor;
            limit_backward = v_3;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 69
                    // delete, line 69
                    slice_del();
                    // do, line 69
                    v_4 = limit - cursor;
                    lab1: do {
                        // call consonant_pair, line 69
                        if (!r_consonant_pair())
                        {
                            break lab1;
                        }
                    } while (false);
                    cursor = limit - v_4;
                    break;
                case 2:
                    // (, line 71
                    // <-, line 71
                    slice_from("l\u00F8s");
                    break;
            }
            return true;
        }

        private boolean r_undouble() {
            int v_1;
            int v_2;
            // (, line 74
            // setlimit, line 75
            v_1 = limit - cursor;
            // tomark, line 75
            if (cursor < I_p1)
            {
                return false;
            }
            cursor = I_p1;
            v_2 = limit_backward;
            limit_backward = cursor;
            cursor = limit - v_1;
            // (, line 75
            // [, line 75
            ket = cursor;
            if (!(out_grouping_b(g_v, 97, 248)))
            {
                limit_backward = v_2;
                return false;
            }
            // ], line 75
            bra = cursor;
            // -> ch, line 75
            S_ch = slice_to(S_ch);
            limit_backward = v_2;
            // name ch, line 76
            if (!(eq_v_b(S_ch)))
            {
                return false;
            }
            // delete, line 77
            slice_del();
            return true;
        }

        public boolean stem() {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            // (, line 81
            // do, line 83
            v_1 = cursor;
            lab0: do {
                // call mark_regions, line 83
                if (!r_mark_regions())
                {
                    break lab0;
                }
            } while (false);
            cursor = v_1;
            // backwards, line 84
            limit_backward = cursor; cursor = limit;
            // (, line 84
            // do, line 85
            v_2 = limit - cursor;
            lab1: do {
                // call main_suffix, line 85
                if (!r_main_suffix())
                {
                    break lab1;
                }
            } while (false);
            cursor = limit - v_2;
            // do, line 86
            v_3 = limit - cursor;
            lab2: do {
                // call consonant_pair, line 86
                if (!r_consonant_pair())
                {
                    break lab2;
                }
            } while (false);
            cursor = limit - v_3;
            // do, line 87
            v_4 = limit - cursor;
            lab3: do {
                // call other_suffix, line 87
                if (!r_other_suffix())
                {
                    break lab3;
                }
            } while (false);
            cursor = limit - v_4;
            // do, line 88
            v_5 = limit - cursor;
            lab4: do {
                // call undouble, line 88
                if (!r_undouble())
                {
                    break lab4;
                }
            } while (false);
            cursor = limit - v_5;
            cursor = limit_backward;            return true;
        }

}

