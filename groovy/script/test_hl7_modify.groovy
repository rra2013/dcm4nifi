import ca.uhn.hl7v2.DefaultHapiContext
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.util.idgenerator.InMemoryIDGenerator;
import ca.uhn.hl7v2.model.v23.segment.MSH;
import ca.uhn.hl7v2.model.v23.segment.EVN;

def flowFile = session.get()
if (!flowFile) {
    return
}

HapiContext context = new DefaultHapiContext();
PipeParser parser = context.getPipeParser();
context.getParserConfiguration().setIdGenerator(new InMemoryIDGenerator());
context.getParserConfiguration().setValidating(false);


flowFile.'Groovy' = true

Message hapiMsg = parser.parse(flowFile.read().getText('UTF-8'));
String version = hapiMsg.getVersion()
flowFile.'Groovy_Version' = version

if (version == "2.3") {
    ca.uhn.hl7v2.model.v23.message.ADT_A04 adtA01 =
            (ca.uhn.hl7v2.model.v23.message.ADT_A04) hapiMsg;
    MSH msh = adtA01.getMSH();
    flowFile.'MSH' = msh.encode()

    EVN evn = adtA01.getEVN();
    evn.getDateTimePlannedEvent().getTimeOfAnEvent().setValue("20250116000000");
}


REL_SUCCESS << flowFile