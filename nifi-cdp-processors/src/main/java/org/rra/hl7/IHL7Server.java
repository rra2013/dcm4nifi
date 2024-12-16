package org.rra.hl7;

public interface IHL7Server {

    void startServer(int port, String msgType, String trigger) throws InterruptedException;

    void stopServer();
}
