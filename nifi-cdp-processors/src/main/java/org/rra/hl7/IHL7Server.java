package org.rra.hl7;

import org.apache.nifi.processor.exception.ProcessException;

public interface IHL7Server {

    void startServer(int port, String msgType, String trigger) throws ProcessException;

    void stopServer();
}
