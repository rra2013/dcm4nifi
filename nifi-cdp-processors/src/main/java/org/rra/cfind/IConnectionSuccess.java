package org.rra.cfind;


import org.dcm4che3.net.Connection;

@FunctionalInterface
public interface IConnectionSuccess {
    void onConnected(Connection remote);
}
