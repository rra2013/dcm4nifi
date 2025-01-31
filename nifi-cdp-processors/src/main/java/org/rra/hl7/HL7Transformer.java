package org.rra.hl7;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.parser.XMLParser;
import ca.uhn.hl7v2.util.idgenerator.InMemoryIDGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.util.IO;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class HL7Transformer {
    private final static HapiContext CONTEXT = new DefaultHapiContext();
    private final static PipeParser PIPE_PARSER = CONTEXT.getPipeParser();
    private final static XMLParser XML_PARSER = CONTEXT.getXMLParser();

    static {
        CONTEXT.getParserConfiguration().setIdGenerator(new InMemoryIDGenerator());
        CONTEXT.getParserConfiguration().setValidating(false);
    }

    public static void transform2xml(InputStream in, OutputStream buffOut) throws Exception {

        try (BufferedInputStream bis = new BufferedInputStream(in)) {
            String result = IOUtils.toString(bis, StandardCharsets.UTF_8);
            //log.debug("Result: \n {}", result.replaceAll("\\r", "\r\n"));
            Message hapiMsg = PIPE_PARSER.parse(result);
            XMLParser xmlParser = CONTEXT.getXMLParser();
            String xmlDoc = xmlParser.encode(hapiMsg, "XML");

            try (ByteArrayInputStream xmlArray = new ByteArrayInputStream(xmlDoc.getBytes(StandardCharsets.UTF_8))) {
                try (BufferedInputStream xmlIn = new BufferedInputStream(xmlArray)) {
                    IO.copy(xmlIn, buffOut);
                }
            }
        }
    }

    public static void transformFromXml(InputStream in, OutputStream buffOut) throws Exception {
        try(BufferedInputStream bis = new BufferedInputStream(in)) {
            String msg_xml = IOUtils.toString(bis, StandardCharsets.UTF_8);
            Message message = XML_PARSER.parse(msg_xml);
            String pipe_msg = PIPE_PARSER.encode(message);
            IOUtils.write(pipe_msg, buffOut, StandardCharsets.UTF_8);
        }
    }

    public static void transform2Json(InputStream in, OutputStream buffOut) throws Exception {

        try (BufferedInputStream bis = new BufferedInputStream(in)) {
            String result = IOUtils.toString(bis, StandardCharsets.UTF_8);
            Message hapiMsg = PIPE_PARSER.parse(result);
            XMLParser xmlParser = CONTEXT.getXMLParser();
            String xmlDoc = xmlParser.encode(hapiMsg, "XML");
            XmlMapper xmlMapper = new XmlMapper();
            JsonNode node = xmlMapper.readTree(xmlDoc.getBytes());
            ObjectMapper mapper = new ObjectMapper();
            String jsonObjectString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
            try (ByteArrayInputStream xmlArray = new ByteArrayInputStream(jsonObjectString.getBytes(StandardCharsets.UTF_8))) {
                try (BufferedInputStream xmlIn = new BufferedInputStream(xmlArray)) {
                    IO.copy(xmlIn, buffOut);
                }
            }
        }
    }

}
