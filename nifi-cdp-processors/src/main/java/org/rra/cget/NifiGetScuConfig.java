package org.rra.cget;

public class NifiGetScuConfig {
    public enum GET_LEVEL{
        PATIENT,
        STUDY,
        SERIES,
        IMAGE
    }
    public static boolean NOT_ASYNC = false;
    public static boolean NOT_PACK_PDV = false;
    public static boolean TCP_DELAY = false;
    public static int CONNECT_TIMEOUT = 1000;
    public static int REQUEST_TIMEOUT = 0;
    public static int ACCEPT_TIMEOUT = 0;
    public static int RELEASE_TIMEOUT = 0;
    public static int SEND_TIMEOUT = 0;
    public static int STORE_TIMEOUT = 0;
    public static int RESPONSE_TIMEOUT = 0;
    public static int IDLE_TIMEOUT = 0;
    public static int SND_BUFFER = 0;
    public static int RCV_BUFFER = 0;

}
