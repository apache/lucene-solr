// This file was generated automatically by the Snowball to Java compiler

package net.sf.snowball.ext;
import net.sf.snowball.SnowballProgram;
import net.sf.snowball.Among;

/**
 * Generated class implementing code defined by a snowball script.
 */
public class EnglishStemmer extends SnowballProgram {

        private Among a_0[] = {
            new Among ( "gener", -1, -1, "", this)
        };

        private Among a_1[] = {
            new Among ( "ied", -1, 2, "", this),
            new Among ( "s", -1, 3, "", this),
            new Among ( "ies", 1, 2, "", this),
            new Among ( "sses", 1, 1, "", this),
            new Among ( "ss", 1, -1, "", this),
            new Among ( "us", 1, -1, "", this)
        };

        private Among a_2[] = {
            new Among ( "", -1, 3, "", this),
            new Among ( "bb", 0, 2, "", this),
            new Among ( "dd", 0, 2, "", this),
            new Among ( "ff", 0, 2, "", this),
            new Among ( "gg", 0, 2, "", this),
            new Among ( "bl", 0, 1, "", this),
            new Among ( "mm", 0, 2, "", this),
            new Among ( "nn", 0, 2, "", this),
            new Among ( "pp", 0, 2, "", this),
            new Among ( "rr", 0, 2, "", this),
            new Among ( "at", 0, 1, "", this),
            new Among ( "tt", 0, 2, "", this),
            new Among ( "iz", 0, 1, "", this)
        };

        private Among a_3[] = {
            new Among ( "ed", -1, 2, "", this),
            new Among ( "eed", 0, 1, "", this),
            new Among ( "ing", -1, 2, "", this),
            new Among ( "edly", -1, 2, "", this),
            new Among ( "eedly", 3, 1, "", this),
            new Among ( "ingly", -1, 2, "", this)
        };

        private Among a_4[] = {
            new Among ( "anci", -1, 3, "", this),
            new Among ( "enci", -1, 2, "", this),
            new Among ( "ogi", -1, 13, "", this),
            new Among ( "li", -1, 16, "", this),
            new Among ( "bli", 3, 12, "", this),
            new Among ( "abli", 4, 4, "", this),
            new Among ( "alli", 3, 8, "", this),
            new Among ( "fulli", 3, 14, "", this),
            new Among ( "lessli", 3, 15, "", this),
            new Among ( "ousli", 3, 10, "", this),
            new Among ( "entli", 3, 5, "", this),
            new Among ( "aliti", -1, 8, "", this),
            new Among ( "biliti", -1, 12, "", this),
            new Among ( "iviti", -1, 11, "", this),
            new Among ( "tional", -1, 1, "", this),
            new Among ( "ational", 14, 7, "", this),
            new Among ( "alism", -1, 8, "", this),
            new Among ( "ation", -1, 7, "", this),
            new Among ( "ization", 17, 6, "", this),
            new Among ( "izer", -1, 6, "", this),
            new Among ( "ator", -1, 7, "", this),
            new Among ( "iveness", -1, 11, "", this),
            new Among ( "fulness", -1, 9, "", this),
            new Among ( "ousness", -1, 10, "", this)
        };

        private Among a_5[] = {
            new Among ( "icate", -1, 4, "", this),
            new Among ( "ative", -1, 6, "", this),
            new Among ( "alize", -1, 3, "", this),
            new Among ( "iciti", -1, 4, "", this),
            new Among ( "ical", -1, 4, "", this),
            new Among ( "tional", -1, 1, "", this),
            new Among ( "ational", 5, 2, "", this),
            new Among ( "ful", -1, 5, "", this),
            new Among ( "ness", -1, 5, "", this)
        };

        private Among a_6[] = {
            new Among ( "ic", -1, 1, "", this),
            new Among ( "ance", -1, 1, "", this),
            new Among ( "ence", -1, 1, "", this),
            new Among ( "able", -1, 1, "", this),
            new Among ( "ible", -1, 1, "", this),
            new Among ( "ate", -1, 1, "", this),
            new Among ( "ive", -1, 1, "", this),
            new Among ( "ize", -1, 1, "", this),
            new Among ( "iti", -1, 1, "", this),
            new Among ( "al", -1, 1, "", this),
            new Among ( "ism", -1, 1, "", this),
            new Among ( "ion", -1, 2, "", this),
            new Among ( "er", -1, 1, "", this),
            new Among ( "ous", -1, 1, "", this),
            new Among ( "ant", -1, 1, "", this),
            new Among ( "ent", -1, 1, "", this),
            new Among ( "ment", 15, 1, "", this),
            new Among ( "ement", 16, 1, "", this)
        };

        private Among a_7[] = {
            new Among ( "e", -1, 1, "", this),
            new Among ( "l", -1, 2, "", this)
        };

        private Among a_8[] = {
            new Among ( "succeed", -1, -1, "", this),
            new Among ( "proceed", -1, -1, "", this),
            new Among ( "exceed", -1, -1, "", this),
            new Among ( "canning", -1, -1, "", this),
            new Among ( "inning", -1, -1, "", this),
            new Among ( "earring", -1, -1, "", this),
            new Among ( "herring", -1, -1, "", this),
            new Among ( "outing", -1, -1, "", this)
        };

        private Among a_9[] = {
            new Among ( "andes", -1, -1, "", this),
            new Among ( "atlas", -1, -1, "", this),
            new Among ( "bias", -1, -1, "", this),
            new Among ( "cosmos", -1, -1, "", this),
            new Among ( "dying", -1, 3, "", this),
            new Among ( "early", -1, 9, "", this),
            new Among ( "gently", -1, 7, "", this),
            new Among ( "howe", -1, -1, "", this),
            new Among ( "idly", -1, 6, "", this),
            new Among ( "lying", -1, 4, "", this),
            new Among ( "news", -1, -1, "", this),
            new Among ( "only", -1, 10, "", this),
            new Among ( "singly", -1, 11, "", this),
            new Among ( "skies", -1, 2, "", this),
            new Among ( "skis", -1, 1, "", this),
            new Among ( "sky", -1, -1, "", this),
            new Among ( "tying", -1, 5, "", this),
            new Among ( "ugly", -1, 8, "", this)
        };

        private static final char g_v[] = {17, 65, 16, 1 };

        private static final char g_v_WXY[] = {1, 17, 65, 208, 1 };

        private static final char g_valid_LI[] = {55, 141, 2 };

        private boolean B_Y_found;
        private int I_p2;
        private int I_p1;

        private void copy_from(EnglishStemmer other) {
            B_Y_found = other.B_Y_found;
            I_p2 = other.I_p2;
            I_p1 = other.I_p1;
            super.copy_from(other);
        }

        private boolean r_prelude() {
            int v_1;
            int v_2;
            int v_3;
            int v_4;
            // (, line 23
            // unset Y_found, line 24
            B_Y_found = false;
            // do, line 25
            v_1 = cursor;
            lab0: do {
                // (, line 25
                // [, line 25
                bra = cursor;
                // literal, line 25
                if (!(eq_s(1, "y")))
                {
                    break lab0;
                }
                // ], line 25
                ket = cursor;
                if (!(in_grouping(g_v, 97, 121)))
                {
                    break lab0;
                }
                // <-, line 25
                slice_from("Y");
                // set Y_found, line 25
                B_Y_found = true;
            } while (false);
            cursor = v_1;
            // do, line 26
            v_2 = cursor;
            lab1: do {
                // repeat, line 26
                replab2: while(true)
                {
                    v_3 = cursor;
                    lab3: do {
                        // (, line 26
                        // goto, line 26
                        golab4: while(true)
                        {
                            v_4 = cursor;
                            lab5: do {
                                // (, line 26
                                if (!(in_grouping(g_v, 97, 121)))
                                {
                                    break lab5;
                                }
                                // [, line 26
                                bra = cursor;
                                // literal, line 26
                                if (!(eq_s(1, "y")))
                                {
                                    break lab5;
                                }
                                // ], line 26
                                ket = cursor;
                                cursor = v_4;
                                break golab4;
                            } while (false);
                            cursor = v_4;
                            if (cursor >= limit)
                            {
                                break lab3;
                            }
                            cursor++;
                        }
                        // <-, line 26
                        slice_from("Y");
                        // set Y_found, line 26
                        B_Y_found = true;
                        continue replab2;
                    } while (false);
                    cursor = v_3;
                    break replab2;
                }
            } while (false);
            cursor = v_2;
            return true;
        }

        private boolean r_mark_regions() {
            int v_1;
            int v_2;
            // (, line 29
            I_p1 = limit;
            I_p2 = limit;
            // do, line 32
            v_1 = cursor;
            lab0: do {
                // (, line 32
                // or, line 36
                lab1: do {
                    v_2 = cursor;
                    lab2: do {
                        // among, line 33
                        if (find_among(a_0, 1) == 0)
                        {
                            break lab2;
                        }
                        break lab1;
                    } while (false);
                    cursor = v_2;
                    // (, line 36
                    // gopast, line 36
                    golab3: while(true)
                    {
                        lab4: do {
                            if (!(in_grouping(g_v, 97, 121)))
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
                    // gopast, line 36
                    golab5: while(true)
                    {
                        lab6: do {
                            if (!(out_grouping(g_v, 97, 121)))
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
                } while (false);
                // setmark p1, line 37
                I_p1 = cursor;
                // gopast, line 38
                golab7: while(true)
                {
                    lab8: do {
                        if (!(in_grouping(g_v, 97, 121)))
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
                // gopast, line 38
                golab9: while(true)
                {
                    lab10: do {
                        if (!(out_grouping(g_v, 97, 121)))
                        {
                            break lab10;
                        }
                        break golab9;
                    } while (false);
                    if (cursor >= limit)
                    {
                        break lab0;
                    }
                    cursor++;
                }
                // setmark p2, line 38
                I_p2 = cursor;
            } while (false);
            cursor = v_1;
            return true;
        }

        private boolean r_shortv() {
            int v_1;
            // (, line 44
            // or, line 46
            lab0: do {
                v_1 = limit - cursor;
                lab1: do {
                    // (, line 45
                    if (!(out_grouping_b(g_v_WXY, 89, 121)))
                    {
                        break lab1;
                    }
                    if (!(in_grouping_b(g_v, 97, 121)))
                    {
                        break lab1;
                    }
                    if (!(out_grouping_b(g_v, 97, 121)))
                    {
                        break lab1;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_1;
                // (, line 47
                if (!(out_grouping_b(g_v, 97, 121)))
                {
                    return false;
                }
                if (!(in_grouping_b(g_v, 97, 121)))
                {
                    return false;
                }
                // atlimit, line 47
                if (cursor > limit_backward)
                {
                    return false;
                }
            } while (false);
            return true;
        }

        private boolean r_R1() {
            if (!(I_p1 <= cursor))
            {
                return false;
            }
            return true;
        }

        private boolean r_R2() {
            if (!(I_p2 <= cursor))
            {
                return false;
            }
            return true;
        }

        private boolean r_Step_1a() {
            int among_var;
            int v_1;
            // (, line 53
            // [, line 54
            ket = cursor;
            // substring, line 54
            among_var = find_among_b(a_1, 6);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 54
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 55
                    // <-, line 55
                    slice_from("ss");
                    break;
                case 2:
                    // (, line 57
                    // or, line 57
                    lab0: do {
                        v_1 = limit - cursor;
                        lab1: do {
                            // (, line 57
                            // next, line 57
                            if (cursor <= limit_backward)
                            {
                                break lab1;
                            }
                            cursor--;
                            // atlimit, line 57
                            if (cursor > limit_backward)
                            {
                                break lab1;
                            }
                            // <-, line 57
                            slice_from("ie");
                            break lab0;
                        } while (false);
                        cursor = limit - v_1;
                        // <-, line 57
                        slice_from("i");
                    } while (false);
                    break;
                case 3:
                    // (, line 58
                    // next, line 58
                    if (cursor <= limit_backward)
                    {
                        return false;
                    }
                    cursor--;
                    // gopast, line 58
                    golab2: while(true)
                    {
                        lab3: do {
                            if (!(in_grouping_b(g_v, 97, 121)))
                            {
                                break lab3;
                            }
                            break golab2;
                        } while (false);
                        if (cursor <= limit_backward)
                        {
                            return false;
                        }
                        cursor--;
                    }
                    // delete, line 58
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_Step_1b() {
            int among_var;
            int v_1;
            int v_3;
            int v_4;
            // (, line 63
            // [, line 64
            ket = cursor;
            // substring, line 64
            among_var = find_among_b(a_3, 6);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 64
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 66
                    // call R1, line 66
                    if (!r_R1())
                    {
                        return false;
                    }
                    // <-, line 66
                    slice_from("ee");
                    break;
                case 2:
                    // (, line 68
                    // test, line 69
                    v_1 = limit - cursor;
                    // gopast, line 69
                    golab0: while(true)
                    {
                        lab1: do {
                            if (!(in_grouping_b(g_v, 97, 121)))
                            {
                                break lab1;
                            }
                            break golab0;
                        } while (false);
                        if (cursor <= limit_backward)
                        {
                            return false;
                        }
                        cursor--;
                    }
                    cursor = limit - v_1;
                    // delete, line 69
                    slice_del();
                    // test, line 70
                    v_3 = limit - cursor;
                    // substring, line 70
                    among_var = find_among_b(a_2, 13);
                    if (among_var == 0)
                    {
                        return false;
                    }
                    cursor = limit - v_3;
                    switch(among_var) {
                        case 0:
                            return false;
                        case 1:
                            // (, line 72
                            // <+, line 72
                            {
                                int c = cursor;
                                insert(cursor, cursor, "e");
                                cursor = c;
                            }
                            break;
                        case 2:
                            // (, line 75
                            // [, line 75
                            ket = cursor;
                            // next, line 75
                            if (cursor <= limit_backward)
                            {
                                return false;
                            }
                            cursor--;
                            // ], line 75
                            bra = cursor;
                            // delete, line 75
                            slice_del();
                            break;
                        case 3:
                            // (, line 76
                            // atmark, line 76
                            if (cursor != I_p1)
                            {
                                return false;
                            }
                            // test, line 76
                            v_4 = limit - cursor;
                            // call shortv, line 76
                            if (!r_shortv())
                            {
                                return false;
                            }
                            cursor = limit - v_4;
                            // <+, line 76
                            {
                                int c = cursor;
                                insert(cursor, cursor, "e");
                                cursor = c;
                            }
                            break;
                    }
                    break;
            }
            return true;
        }

        private boolean r_Step_1c() {
            int v_1;
            int v_2;
            // (, line 82
            // [, line 83
            ket = cursor;
            // or, line 83
            lab0: do {
                v_1 = limit - cursor;
                lab1: do {
                    // literal, line 83
                    if (!(eq_s_b(1, "y")))
                    {
                        break lab1;
                    }
                    break lab0;
                } while (false);
                cursor = limit - v_1;
                // literal, line 83
                if (!(eq_s_b(1, "Y")))
                {
                    return false;
                }
            } while (false);
            // ], line 83
            bra = cursor;
            if (!(out_grouping_b(g_v, 97, 121)))
            {
                return false;
            }
            // not, line 84
            {
                v_2 = limit - cursor;
                lab2: do {
                    // atlimit, line 84
                    if (cursor > limit_backward)
                    {
                        break lab2;
                    }
                    return false;
                } while (false);
                cursor = limit - v_2;
            }
            // <-, line 85
            slice_from("i");
            return true;
        }

        private boolean r_Step_2() {
            int among_var;
            // (, line 88
            // [, line 89
            ket = cursor;
            // substring, line 89
            among_var = find_among_b(a_4, 24);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 89
            bra = cursor;
            // call R1, line 89
            if (!r_R1())
            {
                return false;
            }
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 90
                    // <-, line 90
                    slice_from("tion");
                    break;
                case 2:
                    // (, line 91
                    // <-, line 91
                    slice_from("ence");
                    break;
                case 3:
                    // (, line 92
                    // <-, line 92
                    slice_from("ance");
                    break;
                case 4:
                    // (, line 93
                    // <-, line 93
                    slice_from("able");
                    break;
                case 5:
                    // (, line 94
                    // <-, line 94
                    slice_from("ent");
                    break;
                case 6:
                    // (, line 96
                    // <-, line 96
                    slice_from("ize");
                    break;
                case 7:
                    // (, line 98
                    // <-, line 98
                    slice_from("ate");
                    break;
                case 8:
                    // (, line 100
                    // <-, line 100
                    slice_from("al");
                    break;
                case 9:
                    // (, line 101
                    // <-, line 101
                    slice_from("ful");
                    break;
                case 10:
                    // (, line 103
                    // <-, line 103
                    slice_from("ous");
                    break;
                case 11:
                    // (, line 105
                    // <-, line 105
                    slice_from("ive");
                    break;
                case 12:
                    // (, line 107
                    // <-, line 107
                    slice_from("ble");
                    break;
                case 13:
                    // (, line 108
                    // literal, line 108
                    if (!(eq_s_b(1, "l")))
                    {
                        return false;
                    }
                    // <-, line 108
                    slice_from("og");
                    break;
                case 14:
                    // (, line 109
                    // <-, line 109
                    slice_from("ful");
                    break;
                case 15:
                    // (, line 110
                    // <-, line 110
                    slice_from("less");
                    break;
                case 16:
                    // (, line 111
                    if (!(in_grouping_b(g_valid_LI, 99, 116)))
                    {
                        return false;
                    }
                    // delete, line 111
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_Step_3() {
            int among_var;
            // (, line 115
            // [, line 116
            ket = cursor;
            // substring, line 116
            among_var = find_among_b(a_5, 9);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 116
            bra = cursor;
            // call R1, line 116
            if (!r_R1())
            {
                return false;
            }
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 117
                    // <-, line 117
                    slice_from("tion");
                    break;
                case 2:
                    // (, line 118
                    // <-, line 118
                    slice_from("ate");
                    break;
                case 3:
                    // (, line 119
                    // <-, line 119
                    slice_from("al");
                    break;
                case 4:
                    // (, line 121
                    // <-, line 121
                    slice_from("ic");
                    break;
                case 5:
                    // (, line 123
                    // delete, line 123
                    slice_del();
                    break;
                case 6:
                    // (, line 125
                    // call R2, line 125
                    if (!r_R2())
                    {
                        return false;
                    }
                    // delete, line 125
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_Step_4() {
            int among_var;
            int v_1;
            // (, line 129
            // [, line 130
            ket = cursor;
            // substring, line 130
            among_var = find_among_b(a_6, 18);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 130
            bra = cursor;
            // call R2, line 130
            if (!r_R2())
            {
                return false;
            }
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 133
                    // delete, line 133
                    slice_del();
                    break;
                case 2:
                    // (, line 134
                    // or, line 134
                    lab0: do {
                        v_1 = limit - cursor;
                        lab1: do {
                            // literal, line 134
                            if (!(eq_s_b(1, "s")))
                            {
                                break lab1;
                            }
                            break lab0;
                        } while (false);
                        cursor = limit - v_1;
                        // literal, line 134
                        if (!(eq_s_b(1, "t")))
                        {
                            return false;
                        }
                    } while (false);
                    // delete, line 134
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_Step_5() {
            int among_var;
            int v_1;
            int v_2;
            // (, line 138
            // [, line 139
            ket = cursor;
            // substring, line 139
            among_var = find_among_b(a_7, 2);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 139
            bra = cursor;
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 140
                    // or, line 140
                    lab0: do {
                        v_1 = limit - cursor;
                        lab1: do {
                            // call R2, line 140
                            if (!r_R2())
                            {
                                break lab1;
                            }
                            break lab0;
                        } while (false);
                        cursor = limit - v_1;
                        // (, line 140
                        // call R1, line 140
                        if (!r_R1())
                        {
                            return false;
                        }
                        // not, line 140
                        {
                            v_2 = limit - cursor;
                            lab2: do {
                                // call shortv, line 140
                                if (!r_shortv())
                                {
                                    break lab2;
                                }
                                return false;
                            } while (false);
                            cursor = limit - v_2;
                        }
                    } while (false);
                    // delete, line 140
                    slice_del();
                    break;
                case 2:
                    // (, line 141
                    // call R2, line 141
                    if (!r_R2())
                    {
                        return false;
                    }
                    // literal, line 141
                    if (!(eq_s_b(1, "l")))
                    {
                        return false;
                    }
                    // delete, line 141
                    slice_del();
                    break;
            }
            return true;
        }

        private boolean r_exception2() {
            // (, line 145
            // [, line 147
            ket = cursor;
            // substring, line 147
            if (find_among_b(a_8, 8) == 0)
            {
                return false;
            }
            // ], line 147
            bra = cursor;
            // atlimit, line 147
            if (cursor > limit_backward)
            {
                return false;
            }
            return true;
        }

        private boolean r_exception1() {
            int among_var;
            // (, line 157
            // [, line 159
            bra = cursor;
            // substring, line 159
            among_var = find_among(a_9, 18);
            if (among_var == 0)
            {
                return false;
            }
            // ], line 159
            ket = cursor;
            // atlimit, line 159
            if (cursor < limit)
            {
                return false;
            }
            switch(among_var) {
                case 0:
                    return false;
                case 1:
                    // (, line 163
                    // <-, line 163
                    slice_from("ski");
                    break;
                case 2:
                    // (, line 164
                    // <-, line 164
                    slice_from("sky");
                    break;
                case 3:
                    // (, line 165
                    // <-, line 165
                    slice_from("die");
                    break;
                case 4:
                    // (, line 166
                    // <-, line 166
                    slice_from("lie");
                    break;
                case 5:
                    // (, line 167
                    // <-, line 167
                    slice_from("tie");
                    break;
                case 6:
                    // (, line 171
                    // <-, line 171
                    slice_from("idl");
                    break;
                case 7:
                    // (, line 172
                    // <-, line 172
                    slice_from("gentl");
                    break;
                case 8:
                    // (, line 173
                    // <-, line 173
                    slice_from("ugli");
                    break;
                case 9:
                    // (, line 174
                    // <-, line 174
                    slice_from("earli");
                    break;
                case 10:
                    // (, line 175
                    // <-, line 175
                    slice_from("onli");
                    break;
                case 11:
                    // (, line 176
                    // <-, line 176
                    slice_from("singl");
                    break;
            }
            return true;
        }

        private boolean r_postlude() {
            int v_1;
            int v_2;
            // (, line 192
            // Boolean test Y_found, line 192
            if (!(B_Y_found))
            {
                return false;
            }
            // repeat, line 192
            replab0: while(true)
            {
                v_1 = cursor;
                lab1: do {
                    // (, line 192
                    // goto, line 192
                    golab2: while(true)
                    {
                        v_2 = cursor;
                        lab3: do {
                            // (, line 192
                            // [, line 192
                            bra = cursor;
                            // literal, line 192
                            if (!(eq_s(1, "Y")))
                            {
                                break lab3;
                            }
                            // ], line 192
                            ket = cursor;
                            cursor = v_2;
                            break golab2;
                        } while (false);
                        cursor = v_2;
                        if (cursor >= limit)
                        {
                            break lab1;
                        }
                        cursor++;
                    }
                    // <-, line 192
                    slice_from("y");
                    continue replab0;
                } while (false);
                cursor = v_1;
                break replab0;
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
            int v_11;
            int v_12;
            int v_13;
            // (, line 194
            // or, line 196
            lab0: do {
                v_1 = cursor;
                lab1: do {
                    // call exception1, line 196
                    if (!r_exception1())
                    {
                        break lab1;
                    }
                    break lab0;
                } while (false);
                cursor = v_1;
                // (, line 196
                // test, line 198
                v_2 = cursor;
                // hop, line 198
                {
                    int c = cursor + 3;
                    if (0 > c || c > limit)
                    {
                        return false;
                    }
                    cursor = c;
                }
                cursor = v_2;
                // do, line 199
                v_3 = cursor;
                lab2: do {
                    // call prelude, line 199
                    if (!r_prelude())
                    {
                        break lab2;
                    }
                } while (false);
                cursor = v_3;
                // do, line 200
                v_4 = cursor;
                lab3: do {
                    // call mark_regions, line 200
                    if (!r_mark_regions())
                    {
                        break lab3;
                    }
                } while (false);
                cursor = v_4;
                // backwards, line 201
                limit_backward = cursor; cursor = limit;
                // (, line 201
                // do, line 203
                v_5 = limit - cursor;
                lab4: do {
                    // call Step_1a, line 203
                    if (!r_Step_1a())
                    {
                        break lab4;
                    }
                } while (false);
                cursor = limit - v_5;
                // or, line 205
                lab5: do {
                    v_6 = limit - cursor;
                    lab6: do {
                        // call exception2, line 205
                        if (!r_exception2())
                        {
                            break lab6;
                        }
                        break lab5;
                    } while (false);
                    cursor = limit - v_6;
                    // (, line 205
                    // do, line 207
                    v_7 = limit - cursor;
                    lab7: do {
                        // call Step_1b, line 207
                        if (!r_Step_1b())
                        {
                            break lab7;
                        }
                    } while (false);
                    cursor = limit - v_7;
                    // do, line 208
                    v_8 = limit - cursor;
                    lab8: do {
                        // call Step_1c, line 208
                        if (!r_Step_1c())
                        {
                            break lab8;
                        }
                    } while (false);
                    cursor = limit - v_8;
                    // do, line 210
                    v_9 = limit - cursor;
                    lab9: do {
                        // call Step_2, line 210
                        if (!r_Step_2())
                        {
                            break lab9;
                        }
                    } while (false);
                    cursor = limit - v_9;
                    // do, line 211
                    v_10 = limit - cursor;
                    lab10: do {
                        // call Step_3, line 211
                        if (!r_Step_3())
                        {
                            break lab10;
                        }
                    } while (false);
                    cursor = limit - v_10;
                    // do, line 212
                    v_11 = limit - cursor;
                    lab11: do {
                        // call Step_4, line 212
                        if (!r_Step_4())
                        {
                            break lab11;
                        }
                    } while (false);
                    cursor = limit - v_11;
                    // do, line 214
                    v_12 = limit - cursor;
                    lab12: do {
                        // call Step_5, line 214
                        if (!r_Step_5())
                        {
                            break lab12;
                        }
                    } while (false);
                    cursor = limit - v_12;
                } while (false);
                cursor = limit_backward;                // do, line 217
                v_13 = cursor;
                lab13: do {
                    // call postlude, line 217
                    if (!r_postlude())
                    {
                        break lab13;
                    }
                } while (false);
                cursor = v_13;
            } while (false);
            return true;
        }

}

