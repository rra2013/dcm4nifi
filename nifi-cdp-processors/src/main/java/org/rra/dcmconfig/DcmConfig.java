package org.rra.dcmconfig;

public class DcmConfig {
    public enum LEVEL {
        STUDY,
        SERIES,
        IMAGE
    }
    public LEVEL FIND_LEVEL;
    public boolean NOT_ASYNC = false;
    public boolean NOT_PACK_PDV = false;
    public boolean TCP_DELAY = false;
    public int CONNECT_TIMEOUT = 30000;
    public int REQUEST_TIMEOUT = 20000;
    public int ACCEPT_TIMEOUT = 20000;
    public int RELEASE_TIMEOUT = 2000;
    public int SEND_TIMEOUT = 8000;
    public int STORE_TIMEOUT = 8000;
    public int RESPONSE_TIMEOUT = 8000;
    public int IDLE_TIMEOUT = 8000;
    public int SND_BUFFER = 0;
    public int RCV_BUFFER = 0;
}
