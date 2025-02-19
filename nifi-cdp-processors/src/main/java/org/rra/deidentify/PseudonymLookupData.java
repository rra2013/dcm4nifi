package org.rra.deidentify;

import lombok.Data;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

@Data
public class PseudonymLookupData {
    final String pid;
    final String prefix;
    final String postfix;

    public PseudonymLookupData(ResultSet resultSet) throws Exception {
        final ResultSetMetaData meta = resultSet.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        if (nrOfColumns <= 0 || nrOfColumns < 3) {
            throw new Exception("Found wrong number of columns in result set.");
        }
        if (resultSet.next()) {
            pid = resultSet.getString(resultSet.findColumn("pid"));
            prefix = resultSet.getString(resultSet.findColumn("prefix"));
            postfix = resultSet.getString(resultSet.findColumn("postfix"));
        } else {
            throw new Exception("No results found");
        }
    }
}
