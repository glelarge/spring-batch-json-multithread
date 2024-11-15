package org.springframework.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.NonNull;

public class MyDataProcessor implements ItemProcessor<MyData, MyData> {

    private static final Logger logger = LoggerFactory.getLogger(MyDataProcessor.class);
    private final JdbcTemplate jdbcTemplate;

    public MyDataProcessor(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /*
     * Executed with chunk
     */
    @Override
    public MyData process(@NonNull MyData item) throws Exception {
        logger.info("Processing item: {}", item);

        MyData data = item;

        // Perform some processing with SQL queries

        return data;
    }
}
