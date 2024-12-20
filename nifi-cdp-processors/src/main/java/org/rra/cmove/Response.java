package org.rra.cmove;

import org.dcm4che3.data.Attributes;

@FunctionalInterface
public interface Response {
    void onResponse(Attributes cmd, int status);
}
