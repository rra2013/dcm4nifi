package org.rra.hl7;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;

/**
 * Minimal raw MLLP client for tests.
 * <p>
 * Deliberately not HAPI's client: HAPI caches connections per host/port in its ConnectionHub,
 * which hides reconnects / restarts and makes truly parallel clients impossible.
 * This client gives full control over bytes, charset and framing (incl. broken frames).
 */
public final class MllpTestClient implements Closeable {

    public static final byte START_BLOCK = 0x0b;
    public static final byte END_BLOCK = 0x1c;
    public static final byte CR = 0x0d;

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final Charset charset;

    public MllpTestClient(int port, Charset charset) throws IOException {
        this.charset = charset;
        this.socket = new Socket();
        this.socket.connect(new InetSocketAddress("localhost", port), 5_000);
        this.socket.setSoTimeout(30_000);
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
    }

    /** Sends one framed message and blocks until the framed response arrives. */
    public String sendAndReceive(String message) throws IOException {
        send(message);
        return readFrame();
    }

    public void send(String message) throws IOException {
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        frame.write(START_BLOCK);
        frame.write(message.getBytes(charset));
        frame.write(END_BLOCK);
        frame.write(CR);
        out.write(frame.toByteArray());
        out.flush();
    }

    public void setReadTimeout(int millis) throws IOException {
        socket.setSoTimeout(millis);
    }

    public void sendRaw(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    /** Reads one MLLP frame; returns null if the server closed the connection before a frame arrived. */
    public String readFrame() throws IOException {
        int b;
        do {
            b = in.read();
            if (b == -1) return null;
        } while (b != START_BLOCK);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        while (true) {
            b = in.read();
            if (b == -1) return null;
            if (b == END_BLOCK) {
                int next = in.read();
                if (next == CR || next == -1) break;
                body.write(b);
                body.write(next);
                continue;
            }
            body.write(b);
        }
        return body.toString(charset);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    // ------------------------------------------------------------------ helpers

    /** MSA-1 (AA, AE, AR, CA, CE, CR) of an ACK, or null if there is no MSA segment. */
    public static String ackCode(String ack) {
        return msaField(ack, 1);
    }

    /** MSA-2: control id of the acknowledged message. */
    public static String ackControlId(String ack) {
        return msaField(ack, 2);
    }

    private static String msaField(String ack, int index) {
        if (ack == null) return null;
        for (String segment : ack.split("[\r\n]+")) {
            if (segment.startsWith("MSA|")) {
                String[] fields = segment.split("\\|", -1);
                return fields.length > index ? fields[index] : null;
            }
        }
        return null;
    }

    public static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    /** True if nobody listens on the port any more (i.e. it can be bound again). */
    public static boolean isPortFree(int port) {
        try (ServerSocket s = new ServerSocket()) {
            s.setReuseAddress(true);
            s.bind(new InetSocketAddress(port));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    // ------------------------------------------------------------------ test messages

    public static String adtA01(String controlId) {
        return message("ADT^A01", controlId, "2.5", "HIS", "RIH", "EKG", "EKG", null, "Müller^Jürg");
    }

    public static String message(String type, String controlId, String version,
                                 String sendApp, String sendFac, String recApp, String recFac,
                                 String msh18, String patientName) {
        String msh = "MSH|^~\\&|" + sendApp + "|" + sendFac + "|" + recApp + "|" + recFac
                + "|20261110120000||" + type + "|" + controlId + "|P|" + version
                + (msh18 != null ? "||||||" + msh18 : "");   // MSH-13..17 empty, MSH-18
        return msh + "\r"
                + "EVN|A01|20261110120000\r"
                + "PID|1||12345^^^HOSP^PI||" + patientName + "||19700101|M\r"
                + "PV1|1|I|STATION^101^1\r";
    }

    public static String ormO01(String controlId) {
        return "MSH|^~\\&|RIS|INSEL|DCMA|IDSC|20261110120000||ORM^O01|" + controlId + "|P|2.5\r"
                + "PID|1||12345^^^HOSP^PI||Muster^Max||19700101|M\r"
                + "ORC|NW|ORD" + controlId + "\r"
                + "OBR|1|ORD" + controlId + "||CT^CT Thorax\r";
    }

    public static String oruR01(String controlId) {
        return "MSH|^~\\&|LAB|INSEL|EKG|EKG|20261110120000||ORU^R01|" + controlId + "|P|2.5\r"
                + "PID|1||12345^^^HOSP^PI||Muster^Max||19700101|M\r"
                + "OBR|1|ORD1||GLU^Glucose\r"
                + "OBX|1|NM|GLU^Glucose||5.4|mmol/L\r";
    }
}
