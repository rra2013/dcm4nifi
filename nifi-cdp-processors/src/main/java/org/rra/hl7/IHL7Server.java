package org.rra.hl7;

import org.apache.nifi.processor.exception.ProcessException;

import java.nio.charset.Charset;

public interface IHL7Server {

    /**
     * @param charset fallback character set of the MLLP stream; MSH-18 of a message takes precedence if present
     */
    void startServer(int port, String msgType, String trigger, Charset charset) throws ProcessException;

    void stopServer();
}
