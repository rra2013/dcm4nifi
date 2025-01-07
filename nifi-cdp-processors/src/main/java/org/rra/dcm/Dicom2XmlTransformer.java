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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;

import static org.dcm4che3.io.DicomInputStream.IncludeBulkData;

public class Dicom2XmlTransformer {

    private static final String XML_1_0 = "1.0";
    private static final String xmlVersion = XML_1_0;

    public static void transform(InputStream in, OutputStream out, boolean includeBulkData, String xsltPath) throws IOException, TransformerConfigurationException {

        DicomInputStream dis = new DicomInputStream(in);
        try {
            parse(dis, out, includeBulkData, xsltPath);
        } catch (TransformerConfigurationException e) {
            throw e;
        } finally {
            dis.close();
        }

    }

    private static void parse(DicomInputStream dis, OutputStream out, boolean includeBulkData, String xsltPath) throws IOException,
            TransformerConfigurationException {

        BasicBulkDataDescriptor bulkDataDescriptor = new BasicBulkDataDescriptor();
        bulkDataDescriptor.excludeDefaults(false);
        if (includeBulkData){
            dis.setIncludeBulkData(IncludeBulkData.YES);
        }else{
            dis.setIncludeBulkData(IncludeBulkData.NO);
        }
        dis.setBulkDataDescriptor(bulkDataDescriptor);
        dis.setBulkDataDirectory(null);
        dis.setBulkDataFilePrefix("blk");
        dis.setBulkDataFileSuffix(null);
        dis.setConcatenateBulkDataFiles(false);
        TransformerHandler th = getTransformerHandler(toXSLTURL(xsltPath));
        Transformer t = th.getTransformer();
        boolean indent = false;
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
        saxWriter.setIncludeNamespaceDeclaration(false);
        dis.setDicomInputHandler(saxWriter);
        dis.readDataset();
    }

    private static String toXSLTURL(String xsltPath) {
        if (null == xsltPath) {
            return null;
        }
        return toURL(xsltPath);
    }
    private static String toURL(String fileOrURL) {
        try {
            new URL(fileOrURL);
            return fileOrURL;
        } catch (MalformedURLException e) {
            return new File(fileOrURL).toURI().toString();
        }
    }
    private static TransformerHandler getTransformerHandler(String xsltURL)
            throws TransformerConfigurationException {
        SAXTransformerFactory tf = (SAXTransformerFactory)
                TransformerFactory.newInstance();

        if (xsltURL == null)
            return tf.newTransformerHandler();

        TransformerHandler th = tf.newTransformerHandler(new StreamSource(xsltURL));
        return th;
    }
}
