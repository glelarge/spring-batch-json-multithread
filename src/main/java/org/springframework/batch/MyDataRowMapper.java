
package org.springframework.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MyDataRowMapper implements RowMapper<MyData> {

    private static final Logger logger = LoggerFactory.getLogger(MyDataRowMapper.class);

    @Override
    public MyData mapRow(ResultSet rs, int rowNum) throws SQLException {

        MyData myData = new MyData(
            rs.getInt("CODE"),
            rs.getString("REF"),
            rs.getInt("TYPE"),
            rs.getInt("NATURE"),
            rs.getInt("ETAT"),
            rs.getString("REF2")
        );

        logger.debug("Mapped row: {}", myData);

        return myData;
    }
}