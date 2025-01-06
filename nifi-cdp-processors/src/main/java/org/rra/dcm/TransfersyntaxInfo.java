package org.rra.dcm;

import org.dcm4che3.data.UID;

import java.util.Arrays;
import java.util.List;

public class TransfersyntaxInfo {

    static final String[] UNCOMPRESSED_TSUIDS = {
            UID.ImplicitVRLittleEndian,
            UID.ExplicitVRLittleEndian,
            UID.ExplicitVRBigEndian
    };

    private TransfersyntaxInfo() {
    }

    public static boolean isUncompressed(String transfersyntax) {
        List<String> ts = Arrays.asList(UNCOMPRESSED_TSUIDS);
        return ts.contains(transfersyntax);
    }
}
