package org.rra.deidentify.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.rra.processors.Deidentify;

import java.io.IOException;
import java.io.InputStream;

@Data
@ToString
@Slf4j
public class DeidentifyModel {
    private static DeidentifyModel model = null;
    private int[] X_Tags;
    private int[] X_Institutions;
    private int[] X_Devices;
    private int[] X_Dates;
    private int[] Z_Tags;
    private int[] Z_Institutions;
    private int[] Z_Dates;
    private int[] Z_UID;
    private int[] D_Tags;
    private int[] D_Devices;
    private int[] D_Dates;
    private int[] D_Institutions;
    private int[] U_Tags;
    private int[] U_Devices;
    private boolean retain_date;
    private boolean retain_dev;
    private boolean retain_org;
    private boolean retain_uid;
    private boolean retain_pid_hash;

    private DeidentifyModel() {
        //Singleton
    }

    public static DeidentifyModel getModel() {
        if (null != model) return model;

        ObjectMapper mapper = new ObjectMapper();
        try {
            ClassLoader classLoader = Deidentify.class.getClassLoader();
            InputStream asStream = classLoader.getResourceAsStream("deidentify.json");
            model = mapper.readValue(asStream, DeidentifyModel.class);
            log.info("+ + + Read Deidentify Tags OK. + + +");
        } catch (IOException e) {
            model = null;
        }
        return model;
    }
}
