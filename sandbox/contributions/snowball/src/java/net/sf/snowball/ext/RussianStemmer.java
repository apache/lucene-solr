// This file was generated automatically by the Snowball to Java compiler

package net.sf.snowball.ext;
import net.sf.snowball.SnowballProgram;
import net.sf.snowball.Among;

/**
 * Generated class implementing code defined by a snowball script.
 */
public class RussianStemmer extends SnowballProgram {

        private Among a_0[] = {
            new Among ( "\u00D7\u00DB\u00C9", -1, 1, "", this),
            new Among ( "\u00C9\u00D7\u00DB\u00C9", 0, 2, "", this),
            new Among ( "\u00D9\u00D7\u00DB\u00C9", 0, 2, "", this),
            new Among ( "\u00D7", -1, 1, "", this),
            new Among ( "\u00C9\u00D7", 3, 2, "", this),
            new Among ( "\u00D9\u00D7", 3, 2, "", this),
            new Among ( "\u00D7\u00DB\u00C9\u00D3\u00D8", -1, 1, "", this),
            new Among ( "\u00C9\u00D7\u00DB\u00C9\u00D3\u00D8", 6, 2, "", this),
            new Among ( "\u00D9\u00D7\u00DB\u00C9\u00D3\u00D8", 6, 2, "", this)
        };

        private Among a_1[] = {
            new Among ( "\u00C0\u00C0", -1, 1, "", this),
            new Among ( "\u00C5\u00C0", -1, 1, "", this),
            new Among ( "\u00CF\u00C0", -1, 1, "", this),
            new Among ( "\u00D5\u00C0", -1, 1, "", this),
            new Among ( "\u00C5\u00C5", -1, 1, "", this),
            new Among ( "\u00C9\u00C5", -1, 1, "", this),
            new Among ( "\u00CF\u00C5", -1, 1, "", this),
            new Among ( "\u00D9\u00C5", -1, 1, "", this),
            new Among ( "\u00C9\u00C8", -1, 1, "", this),
            new Among ( "\u00D9\u00C8", -1, 1, "", this),
            new Among ( "\u00C9\u00CD\u00C9", -1, 1, "", this),
            new Among ( "\u00D9\u00CD\u00C9", -1, 1, "", this),
            new Among ( "\u00C5\u00CA", -1, 1, "", this),
            new Among ( "\u00C9\u00CA", -1, 1, "", this),
            new Among ( "\u00CF\u00CA", -1, 1, "", this),
            new Among ( "\u00D9\u00CA", -1, 1, "", this),
            new Among ( "\u00C5\u00CD", -1, 1, "", this),
            new Among ( "\u00C9\u00CD", -1, 1, "", this),
            new Among ( "\u00CF\u00CD", -1, 1, "", this),
            new Among ( "\u00D9\u00CD", -1, 1, "", this),
            new Among ( "\u00C5\u00C7\u00CF", -1, 1, "", this),
            new Among ( "\u00CF\u00C7\u00CF", -1, 1, "", this),
            new Among ( "\u00C1\u00D1", -1, 1, "", this),
            new Among ( "\u00D1\u00D1", -1, 1, "", this),
            new Among ( "\u00C5\u00CD\u00D5", -1, 1, "", this),
            new Among ( "\u00CF\u00CD\u00D5", -1, 1, "", this)
        };

        private Among a_2[] = {
            new Among ( "\u00C5\u00CD", -1, 1, "", this),
            new Among ( "\u00CE\u00CE", -1, 1, "", this),
            new Among ( "\u00D7\u00DB", -1, 1, "", this),
            new Among ( "\u00C9\u00D7\u00DB", 2, 2, "", this),
            new Among ( "\u00D9\u00D7\u00DB", 2, 2, "", this),
            new Among ( "\u00DD", -1, 1, "", this),
            new Among ( "\u00C0\u00DD", 5, 1, "", this),
            new Among ( "\u00D5\u00C0\u00DD", 6, 2, "", this)
        };

        private Among a_3[] = {
            new Among ( "\u00D3\u00D1", -1, 1, "", this),
            new Among ( "\u00D3\u00D8", -1, 1, "", this)
        };

        private Among a_4[] = {
            new Among ( "\u00C0", -1, 2, "", this),
            new Among ( "\u00D5\u00C0", 0, 2, "", this),
            new Among ( "\u00CC\u00C1", -1, 1, "", this),
            new Among ( "\u00C9\u00CC\u00C1", 2, 2, "", this),
            new Among ( "\u00D9\u00CC\u00C1", 2, 2, "", this),
            new Among ( "\u00CE\u00C1", -1, 1, "", this),
            new Among ( "\u00C5\u00CE\u00C1", 5, 2, "", this),
            new Among ( "\u00C5\u00D4\u00C5", -1, 1, "", this),
            new Among ( "\u00C9\u00D4\u00C5", -1, 2, "", this),
            new Among ( "\u00CA\u00D4\u00C5", -1, 1, "", this),
            new Among ( "\u00C5\u00CA\u00D4\u00C5", 9, 2, "", this),
            new Among ( "\u00D5\u00CA\u00D4\u00C5", 9, 2, "", this),
            new Among ( "\u00CC\u00C9", -1, 1, "", this),
            new Among ( "\u00C9\u00CC\u00C9", 12, 2, "", this),
            new Among ( "\u00D9\u00CC\u00C9", 12, 2, "", this),
            new Among ( "\u00CA", -1, 1, "", this),
            new Among ( "\u00C5\u00CA", 15, 2, "", this),
            new Among ( "\u00D5\u00CA", 15, 2, "", this),
            new Among ( "\u00CC", -1, 1, "", this),
            new Among ( "\u00C9\u00CC", 18, 2, "", this),
            new Among ( "\u00D9\u00CC", 18, 2, "", this),
            new Among ( "\u00C5\u00CD", -1, 1, "", this),
            new Among ( "\u00C9\u00CD", -1, 2, "", this),
            new Among ( "\u00D9\u00CD", -1, 2, "", this),
            new Among ( "\u00CE", -1, 1, "", this),
            new Among ( "\u00C5\u00CE", 24, 2, "", this),
            new Among ( "\u00CC\u00CF", -1, 1, "", this),
            new Among ( "\u00C9\u00CC\u00CF", 26, 2, "", this),
            new Among ( "\u00D9\u00CC\u00CF", 26, 2, "", this),
            new Among ( "\u00CE\u00CF", -1, 1, "", this),
            new Among ( "\u00C5\u00CE\u00CF", 29, 2, "", this),
            new Among ( "\u00CE\u00CE\u00CF", 29, 1, "", this),
            new Among ( "\u00C0\u00D4", -1, 1, "", this),
            new Among ( "\u00D5\u00C0\u00D4", 32, 2, "", this),
            new Among ( "\u00C5\u00D4", -1, 1, "", this),
            new Among ( "\u00D5\u00C5\u00D4", 34, 2, "", this),
            new Among ( "\u00C9\u00D4", -1, 2, "", this),
            new Among ( "\u00D1\u00D4", -1, 2, "", this),
            new Among ( "\u00D9\u00D4", -1, 2, "", this),
            new Among ( "\u00D4\u00D8", -1, 1, "", this),
            new Among ( "\u00C9\u00D4\u00D8", 39, 2, "", this),
            new Among ( "\u00D9\u00D4\u00D8", 39, 2, "", this),
            new Among ( "\u00C5\u00DB\u00D8", -1, 1, "", this),
            new Among ( "\u00C9\u00DB\u00D8", -1, 2, "", this),
            new Among ( "\u00CE\u00D9", -1, 1, "", this),
            new Among ( "\u00C5\u00CE\u00D9", 44, 2, "", this)
        };

        private Among a_5[] = {
            new Among ( "\u00C0", -1, 1, "", this),
            new Among ( "\u00C9\u00C0", 0, 1, "", this),
            new Among ( "\u00D8\u00C0", 0, 1, "", this),
            new Among ( "\u00C1", -1, 1, "", this),
            new Among ( "\u00C5", -1, 1, "", this),
            new Among ( "\u00C9\u00C5", 4, 1, "", this),
            new Among ( "\u00D8\u00C5", 4, 1, "", this),
            new Among ( "\u00C1\u00C8", -1, 1, "", this),
            new Among ( "\u00D1\u00C8", -1, 1, "", this),
            new Among ( "\u00C9\u00D1\u00C8", 8, 1, "", this),
            new Among ( "\u00C9", -1, 1, "", this),
            new Among ( "\u00C5\u00C9", 10, 1, "", this),
            new Among ( "\u00C9\u00C9", 10, 1, "", this),
            new Among ( "\u00C1\u00CD\u00C9", 10, 1, "", this),
            new Among ( "\u00D1\u00CD\u00C9", 10, 1, "", this),
            new Among ( "\u00C9\u00D1\u00CD\u00C9", 14, 1, "", this),
            new Among ( "\u00CA", -1, 1, "", this),
            new Among ( "\u00C5\u00CA", 16, 1, "", this),
            new Among ( "\u00C9\u00C5\u00CA", 17, 1, "", this),
            new Among ( "\u00C9\u00CA", 16, 1, "", this),
            new Among ( "\u00CF\u00CA", 16, 1, "", this),
            new Among ( "\u00C1\u00CD", -1, 1, "", this),
            new Among ( "\u00C5\u00CD", -1, 1, "", this),
            new Among ( "\u00C9\u00C5\u00CD", 22, 1, "", this),
            new Among ( "\u00CF\u00CD", -1, 1, "", this),
            new Among ( "\u00D1\u00CD", -1, 1, "", this),
            new Among ( "\u00C9\u00D1\u00CD", 25, 1, "", this),
            new Among ( "\u00CF", -1, 1, "", this),
            new Among ( "\u00D1", -1, 1, "", this),
            new Among ( "\u00C9\u00D1", 28, 1, "", this),
            new Among ( "\u00D8\u00D1", 28, 1, "", this),
            new Among ( "\u00D5", -1, 1, "", this),
            new Among ( "\u00C5\u00D7", -1, 1, "", this),
            new Among ( "\u00CF\u00D7", -1, 1, "", this),
            new Among ( "\u00D8", -1, 1, "", this),
            new Among ( "\u00D9", -1, 1, "", this)
        };

        private Among a_6[] = {
            new Among ( "\u00CF\u00D3\u00D4", -1, 1, "", this),
            new Among ( "\u00CF\u00D3\u00D4\u00D8", -1, 1, "", this)
        };

        private Among a_7[] = {
            new Among ( "\u00C5\u00CA\u00DB\u00C5", -1, 1, "", this),
            new Among ( "\u00CE", -1, 2, "", this),
            new Among ( "\u00D8", -1, 3, "", this),
            new Among ( "\u00C5\u00CA\u00DB", -1, 1, "", this)
        };

        private static final char g_v[] = {35, 130, 34, 18 };

        private int I_p2;
        private int I_pV;

        private void copy_from(RussianStemmer other) {
            I_p2 = other.I_p2;
            I_pV = other.I_pV;
            super.copy_from(other);
        }

        private boolean r_mark_regions() {
            int v_1;
            // (, line 96
            I_pV = limit;
            I_p2 = limit;
            // do, line 100
            v_1 = cursor;
            lab0: do {
                // (, line 100
                // gopast, line 101
                golab1: while(true)
                {
                    lab2: do {
                        if (!(in_grouping(g_v, 192, 220)))
                        {
                            break lab2;
                        }
                        break golab1;
                    } while (false);
                    if (cursor >= limit)
                    {
                        break lab0;
                    }
                    cursor++;
                }
                // setmark pV, line 101
                I_pV = cursor;
                // gopast, line 101
                golab3: while(true)
                {
                    lab4: do {
                        if (!(out_grouping(g_v, 192, 220)))
                        {
                            break lab4;
                        }
                        break golab3;
                    } while (false);
                    if (cursor >= limit)
                    {
                        break lab0;
                    }
                    cursor++;
                }
                // gopast, line 102
                golab5: while(true)
                {
                    lab6: do {
                        if (!(in_grouping(g_v, 192, 220)))
                        {
                            break lab6;
                        }
                        break golab5;
                    } while (false);
                    if (cursor >= limit)
                    {
                        break lab0;
                    }
                    cursor++;
                }
                // gopast, line 102
                golab7: while(true)
                {
                    lab8: do {
                        if (!(out_grouping(g_v, 192, 220)))
                        {
                            break lab8;
                        }
                        break golab7;
                    } while (false);
                    if (cursor >= limit)
                    {
                        break lab0;
                    }
                    cursor++;
                }
                // setmark p2, line 102
                I_p2 = cursor;
            } while (false);
            cursor = v_1;
            return true;
        }

        private boolean r_R2() {
            if (!(I_p2 <= cursor))
            {
                return false;
            }
            return true;
        }

        private boolean r_perfective_gerund() {
            int among_var;
            int v_1;
            // (, line 110
            // [, line 111
            ket = cursor;
            // substring, line 111
            among_var = find_among_b(a_0, 9);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 111
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 115
                    // or, line 115
                    lab0: do {
                        v_1 = limit - cursor;
                        lab1: do {
                            // literal, line 115
                            if (!(eq_s_b(1, "\u00C1")))
                            {
                                break lab1;
                            }
                            break lab0;
                        } while (false);
                        cursor = limit - v_1;
                        // literal, line 115
                        if (!(eq_s_b(1, "\u00D1")))
                        {
                            return false;
                        }
                    } while (false);
                    // delete, line 115
                    slice_del();
                    break;
                case 2:
                    // (, line 122
                    // delete, line 122
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_adjective() {
            int among_var;
            // (, line 126
            // [, line 127
            ket = cursor;
            // substring, line 127
            among_var = find_among_b(a_1, 26);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 127
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 136
                    // delete, line 136
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_adjectival() {
            int among_var;
            int v_1;
            int v_2;
            // (, line 140
            // call adjective, line 141
            if (!r_adjective())
            {
                return false;
            }
            // try, line 148
            v_1 = limit - cursor;
            lab0: do {
                // (, line 148
                // [, line 149
                ket = cursor;
                // substring, line 149
                among_var = find_among_b(a_2, 8);
                if (among_var == 0)
                {
                    cursor = limit - v_1;
                    break lab0;
                }
                // ], line 149
                bra = cursor;
                switch(among_var) {
                    case 0:
                        cursor = limit - v_1;
                        break lab0;
                    case 1:
                        // (, line 154
                        // or, line 154
                        lab1: do {
                            v_2 = limit - cursor;
                            lab2: do {
                                // literal, line 154
                                if (!(eq_s_b(1, "\u00C1")))
                                {
                                    break lab2;
                                }
                                break lab1;
                            } while (false);
                            cursor = limit - v_2;
                            // literal, line 154
                            if (!(eq_s_b(1, "\u00D1")))
                            {
                                cursor = limit - v_1;
                                break lab0;
                            }
                        } while (false);
                        // delete, line 154
                        slice_del();
                        break;
                    case 2:
                        // (, line 161
                        // delete, line 161
                        slice_del();
                        break;
                }
            } while (false);
            return true;
        }

        private boolean r_reflexive() {
            int among_var;
            // (, line 167
            // [, line 168
            ket = cursor;
            // substring, line 168
            among_var = find_among_b(a_3, 2);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 168
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 171
                    // delete, line 171
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_verb() {
            int among_var;
            int v_1;
            // (, line 175
            // [, line 176
            ket = cursor;
            // substring, line 176
            among_var = find_among_b(a_4, 46);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 176
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 182
                    // or, line 182
                    lab0: do {
                        v_1 = limit - cursor;
                        lab1: do {
                            // literal, line 182
                            if (!(eq_s_b(1, "\u00C1")))
                            {
                                break lab1;
                            }
                            break lab0;
                        } while (false);
                        cursor = limit - v_1;
                        // literal, line 182
                        if (!(eq_s_b(1, "\u00D1")))
                        {
                            return false;
                        }
                    } while (false);
                    // delete, line 182
                    slice_del();
                    break;
                case 2:
                    // (, line 190
                    // delete, line 190
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_noun() {
            int among_var;
            // (, line 198
            // [, line 199
            ket = cursor;
            // substring, line 199
            among_var = find_among_b(a_5, 36);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 199
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 206
                    // delete, line 206
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_derivational() {
            int among_var;
            // (, line 214
            // [, line 215
            ket = cursor;
            // substring, line 215
            among_var = find_among_b(a_6, 2);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 215
            bra = cursor;
            // call R2, line 215
            if (!r_R2())
            {
                return false;
            }
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 218
                    // delete, line 218
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_tidy_up() {
            int among_var;
            // (, line 222
            // [, line 223
            ket = cursor;
            // substring, line 223
            among_var = find_among_b(a_7, 4);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 223
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 227
                    // delete, line 227
                    slice_del();
                    // [, line 228
                    ket = cursor;
                    // literal, line 228
                    if (!(eq_s_b(1, "\u00CE")))
                    {
                        return false;
                    }
                    // ], line 228
                    bra = cursor;
                    // literal, line 228
                    if (!(eq_s_b(1, "\u00CE")))
                    {
                        return false;
                    }
                    // delete, line 228
                    slice_del();
                    break;
                case 2:
                    // (, line 231
                    // literal, line 231
                    if (!(eq_s_b(1, "\u00CE")))
                    {
                        return false;
                    }
                    // delete, line 231
                    slice_del();
                    break;
                case 3:
                    // (, line 233
                    // delete, line 233
                    slice_del();
                    break;
            }
            return true;
        }

        public boolean stem() {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            int v_5;
            int v_6;
            int v_7;
            int v_8;
            int v_9;
            int v_10;
            // (, line 238
            // do, line 240
            v_1 = cursor;
            lab0: do {
                // call mark_regions, line 240
                if (!r_mark_regions())
                {
                    break lab0;
                }
            } while (false);
            cursor = v_1;
            // backwards, line 241
            limit_backward = cursor; cursor = limit;
            // setlimit, line 241
            v_2 = limit - cursor;
            // tomark, line 241
            if (cursor < I_pV)
            {
                return false;
            }
            cursor = I_pV;
            v_3 = limit_backward;
            limit_backward = cursor;
            cursor = limit - v_2;
            // (, line 241
            // do, line 242
            v_4 = limit - cursor;
            lab1: do {
                // (, line 242
                // or, line 243
                lab2: do {
                    v_5 = limit - cursor;
                    lab3: do {
                        // call perfective_gerund, line 243
                        if (!r_perfective_gerund())
                        {
                            break lab3;
                        }
                        break lab2;
                    } while (false);
                    cursor = limit - v_5;
                    // (, line 244
                    // try, line 244
                    v_6 = limit - cursor;
                    lab4: do {
                        // call reflexive, line 244
                        if (!r_reflexive())
                        {
                            cursor = limit - v_6;
                            break lab4;
                        }
                    } while (false);
                    // or, line 245
                    lab5: do {
                        v_7 = limit - cursor;
                        lab6: do {
                            // call adjectival, line 245
                            if (!r_adjectival())
                            {
                                break lab6;
                            }
                            break lab5;
                        } while (false);
                        cursor = limit - v_7;
                        lab7: do {
                            // call verb, line 245
                            if (!r_verb())
                            {
                                break lab7;
                            }
                            break lab5;
                        } while (false);
                        cursor = limit - v_7;
                        // call noun, line 245
                        if (!r_noun())
                        {
                            break lab1;
                        }
                    } while (false);
                } while (false);
            } while (false);
            cursor = limit - v_4;
            // try, line 248
            v_8 = limit - cursor;
            lab8: do {
                // (, line 248
                // [, line 248
                ket = cursor;
                // literal, line 248
                if (!(eq_s_b(1, "\u00C9")))
                {
                    cursor = limit - v_8;
                    break lab8;
                }
                // ], line 248
                bra = cursor;
                // delete, line 248
                slice_del();
            } while (false);
            // do, line 251
            v_9 = limit - cursor;
            lab9: do {
                // call derivational, line 251
                if (!r_derivational())
                {
                    break lab9;
                }
            } while (false);
            cursor = limit - v_9;
            // do, line 252
            v_10 = limit - cursor;
            lab10: do {
                // call tidy_up, line 252
                if (!r_tidy_up())
                {
                    break lab10;
                }
            } while (false);
            cursor = limit - v_10;
            limit_backward = v_3;
            cursor = limit_backward;            return true;
        }

}

