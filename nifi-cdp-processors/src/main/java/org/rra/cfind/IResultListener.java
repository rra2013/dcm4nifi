package org.rra.cfind;

import org.dcm4che3.data.Attributes;

@FunctionalInterface
public interface IResultListener {
    void onResult(Attributes data);
}
