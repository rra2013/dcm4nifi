package org.rra.dcm;

import org.dcm4che3.io.BasicBulkDataDescriptor;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.SAXWriter;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.dcm4che3.io.DicomInputStream.IncludeBulkData;

public class Dcm2XmlTransformer {

    private static final String XML_1_0 = "1.0";
    private static final String xsltURL = null;
    private static final String xmlVersion = XML_1_0;

    public static void transform(InputStream in, OutputStream out) throws IOException, TransformerConfigurationException {

        DicomInputStream dis = new DicomInputStream(in);
        try {
            parse(dis, out);
        } catch (TransformerConfigurationException e) {
            throw e;
        } finally {
            dis.close();
        }

    }

    private static void parse(DicomInputStream dis, OutputStream out) throws IOException,
            TransformerConfigurationException {

        BasicBulkDataDescriptor bulkDataDescriptor = new BasicBulkDataDescriptor();
        bulkDataDescriptor.excludeDefaults(false);
        dis.setIncludeBulkData(IncludeBulkData.NO);
        dis.setBulkDataDescriptor(bulkDataDescriptor);
        dis.setBulkDataDirectory(null);
        dis.setBulkDataFilePrefix("blk");
        dis.setBulkDataFileSuffix(null);
        dis.setConcatenateBulkDataFiles(false);
        TransformerHandler th = getTransformerHandler();
        Transformer t = th.getTransformer();
        boolean indent = true;
        if (indent) {
            t.setOutputProperty(OutputKeys.INDENT, "yes");
            t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
        }else{
            t.setOutputProperty(OutputKeys.INDENT, "no");
        }
        t.setOutputProperty(OutputKeys.VERSION, xmlVersion);
        th.setResult(new StreamResult(out));
        SAXWriter saxWriter = new SAXWriter(th);
        saxWriter.setIncludeKeyword(true);
        saxWriter.setIncludeNamespaceDeclaration(true);
        dis.setDicomInputHandler(saxWriter);
        dis.readDataset();
    }

    private static TransformerHandler getTransformerHandler()
            throws TransformerConfigurationException, IOException {
        SAXTransformerFactory tf = (SAXTransformerFactory)
                TransformerFactory.newInstance();
        if (xsltURL == null)
            return tf.newTransformerHandler();

        TransformerHandler th = tf.newTransformerHandler(
                new StreamSource(xsltURL));
        return th;
    }
}
