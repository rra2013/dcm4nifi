package org.rra.deidentify;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

@Slf4j
@Data
public class PseudonymLookupData {
    final String pid;
    final String prefix;
    final String postfix;
    Integer dateShift;

    public PseudonymLookupData(ResultSet resultSet) throws Exception {
        final ResultSetMetaData meta = resultSet.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        if (nrOfColumns <= 0 || nrOfColumns < 3) {
            //date shift is optional
            throw new Exception("Found wrong number of columns in result set.");
        }
        if (resultSet.next()) {
            pid = resultSet.getString(resultSet.findColumn("pid"));
            prefix = resultSet.getString(resultSet.findColumn("prefix"));
            postfix = resultSet.getString(resultSet.findColumn("postfix"));
            try {
                dateShift = resultSet.getInt(resultSet.findColumn("date_shift"));
            } catch (SQLException e) {
                dateShift = null;
            }
        } else {
            throw new Exception("No results found");
        }
    }

}
