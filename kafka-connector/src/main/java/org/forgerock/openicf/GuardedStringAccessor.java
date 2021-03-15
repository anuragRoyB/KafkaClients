package org.forgerock.openicf.connectors.kafka;

import java.util.Arrays;

import org.identityconnectors.common.security.GuardedString;

public class GuardedStringAccessor implements GuardedString.Accessor {
    private char[] array;

    public void access(char[] clearChars) {
        array = new char[clearChars.length];
        System.arraycopy(clearChars, 0, array, 0, array.length);
    }

    public char[] getArray() {
        return array;
    }

    public void clear() {
        Arrays.fill(array, 0, array.length, ' ');
    }
}