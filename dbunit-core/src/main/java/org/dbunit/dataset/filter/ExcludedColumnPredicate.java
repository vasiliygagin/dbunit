/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2004, DbUnit.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
package org.dbunit.dataset.filter;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.dbunit.dataset.Column;

/**
 * @author Manuel Laflamme
 * @since Apr 17, 2004
 * @version $Revision$
 */
public class ExcludedColumnPredicate implements Predicate<Column> {

    private final Set<String> _acceptedNames = new HashSet<>();
    private final Set<String> _acceptedPatterns = new HashSet<>();

    /**
     * @param columnNames
     */
    public ExcludedColumnPredicate(String[] columnNames) {
        for (String columnName : columnNames) {
            addPattern(columnName);
        }
    }

    @Override
    public boolean test(Column column) {
        return this.accept(column.getColumnName());
    }

    /**
     * Add a new accepted pattern. The following wildcard characters are supported:
     * '*' matches zero or more characters, '?' matches one character.
     */
    private void addPattern(String patternName) {
        if (patternName.indexOf("*") != -1 || patternName.indexOf("?") != -1) {
            _acceptedPatterns.add(patternName);
        } else {
            _acceptedNames.add(patternName.toUpperCase());
        }
    }

    public boolean accept(String name) {

        if (_acceptedNames.contains(name.toUpperCase())) {
            return true;
        }

        if (_acceptedPatterns.size() > 0) {
            for (String _acceptedPattern : _acceptedPatterns) {
                if (match(_acceptedPattern, name)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Matches a string against a pattern. The pattern contains two special
     * characters: '*' which means zero or more characters, '?' which means one and
     * only one character.
     *
     * @param pattern the (non-null) pattern to match against
     * @param str     the (non-null) string that must be matched against the pattern
     * @return <code>true</code> when the string matches against the pattern,
     *         <code>false</code> otherwise.
     */
    private boolean match(String pattern, String str) {
        char[] patArr = pattern.toCharArray();
        char[] strArr = str.toCharArray();
        int patIdxStart = 0;
        int patIdxEnd = patArr.length - 1;
        int strIdxStart = 0;
        int strIdxEnd = strArr.length - 1;
        char ch;

        boolean containsStar = false;
        for (char element : patArr) {
            if (element == '*') {
                containsStar = true;
                break;
            }
        }

        if (!containsStar) {
            // No '*'s, so we make a shortcut
            if (patIdxEnd != strIdxEnd) {
                return false; // Pattern and string do not have the same size
            }
            for (int i = 0; i <= patIdxEnd; i++) {
                ch = patArr[i];
                if (ch != '?') {
                    if (Character.toUpperCase(ch) != Character.toUpperCase(strArr[i])) {
                        return false; // Character mismatch
                    }
                }
            }
            return true; // String matches against pattern
        }

        if (patIdxEnd == 0) {
            return true; // Pattern contains only '*', which matches anything
        }

        // Process characters before first star
        while ((ch = patArr[patIdxStart]) != '*' && strIdxStart <= strIdxEnd) {
            if (ch != '?') {
                if (Character.toUpperCase(ch) != Character.toUpperCase(strArr[strIdxStart])) {
                    return false;// Character mismatch
                }
            }
            patIdxStart++;
            strIdxStart++;
        }
        if (strIdxStart > strIdxEnd) {
            // All characters in the string are used. Check if only '*'s are
            // left in the pattern. If so, we succeeded. Otherwise failure.
            for (int i = patIdxStart; i <= patIdxEnd; i++) {
                if (patArr[i] != '*') {
                    return false;
                }
            }
            return true;
        }

        // Process characters after last star
        while ((ch = patArr[patIdxEnd]) != '*' && strIdxStart <= strIdxEnd) {
            if (ch != '?') {
                if (Character.toUpperCase(ch) != Character.toUpperCase(strArr[strIdxEnd])) {
                    return false;// Character mismatch
                }
            }
            patIdxEnd--;
            strIdxEnd--;
        }
        if (strIdxStart > strIdxEnd) {
            // All characters in the string are used. Check if only '*'s are
            // left in the pattern. If so, we succeeded. Otherwise failure.
            for (int i = patIdxStart; i <= patIdxEnd; i++) {
                if (patArr[i] != '*') {
                    return false;
                }
            }
            return true;
        }

        // process pattern between stars. padIdxStart and patIdxEnd point
        // always to a '*'.
        while (patIdxStart != patIdxEnd && strIdxStart <= strIdxEnd) {
            int patIdxTmp = -1;
            for (int i = patIdxStart + 1; i <= patIdxEnd; i++) {
                if (patArr[i] == '*') {
                    patIdxTmp = i;
                    break;
                }
            }
            if (patIdxTmp == patIdxStart + 1) {
                // Two stars next to each other, skip the first one.
                patIdxStart++;
                continue;
            }
            // Find the pattern between padIdxStart & padIdxTmp in str between
            // strIdxStart & strIdxEnd
            int patLength = (patIdxTmp - patIdxStart - 1);
            int strLength = (strIdxEnd - strIdxStart + 1);
            int foundIdx = -1;
            strLoop: for (int i = 0; i <= strLength - patLength; i++) {
                for (int j = 0; j < patLength; j++) {
                    ch = patArr[patIdxStart + j + 1];
                    if (ch != '?') {
                        if (Character.toUpperCase(ch) != Character.toUpperCase(strArr[strIdxStart + i + j])) {
                            continue strLoop;
                        }
                    }
                }

                foundIdx = strIdxStart + i;
                break;
            }

            if (foundIdx == -1) {
                return false;
            }

            patIdxStart = patIdxTmp;
            strIdxStart = foundIdx + patLength;
        }

        // All characters in the string are used. Check if only '*'s are left
        // in the pattern. If so, we succeeded. Otherwise failure.
        for (int i = patIdxStart; i <= patIdxEnd; i++) {
            if (patArr[i] != '*') {
                return false;
            }
        }
        return true;
    }
}
