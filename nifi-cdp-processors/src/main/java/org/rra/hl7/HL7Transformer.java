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
    private final static HapiContext context = new DefaultHapiContext();
    private final static PipeParser parser = context.getPipeParser();

    static {
        context.getParserConfiguration().setIdGenerator(new InMemoryIDGenerator());
        context.getParserConfiguration().setValidating(false);
    }

    public static void transform2xml(InputStream in, OutputStream buffOut) throws Exception {

        try (BufferedInputStream bis = new BufferedInputStream(in)) {
            String result = IOUtils.toString(bis, StandardCharsets.UTF_8);
            //log.debug("Result: \n {}", result.replaceAll("\\r", "\r\n"));
            Message hapiMsg = parser.parse(result);
            XMLParser xmlParser = context.getXMLParser();
            String xmlDoc = xmlParser.encode(hapiMsg, "XML");
            //log.debug(xmlDoc);
            try (ByteArrayInputStream xmlArray = new ByteArrayInputStream(xmlDoc.getBytes(StandardCharsets.UTF_8))) {
                try (BufferedInputStream xmlIn = new BufferedInputStream(xmlArray)) {
                    IO.copy(xmlIn, buffOut);
                }
            }
        }
    }

    public static void transform2Json(InputStream in, OutputStream buffOut) throws Exception {

        try (BufferedInputStream bis = new BufferedInputStream(in)) {
            String result = IOUtils.toString(bis, StandardCharsets.UTF_8);
            Message hapiMsg = parser.parse(result);
            XMLParser xmlParser = context.getXMLParser();
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
