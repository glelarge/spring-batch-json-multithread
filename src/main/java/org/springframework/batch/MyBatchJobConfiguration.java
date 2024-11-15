package org.springframework.batch;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.SynchronizedItemStreamWriter;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@EnableBatchProcessing
public class MyBatchJobConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(MyBatchJobConfiguration.class);

    // Restrict the number of rows to read to short the tests
    public final static String sqlQuery = "SELECT CODE, REF, TYPE, NATURE, ETAT, REF2 FROM mytable where CODE > 10000 and CODE < 10005";

    private final int chunkSize;
    private final String commonFilename;
    private final String jsonFilename;
    private final FileSystemResource jsonFileResource;

    public MyBatchJobConfiguration() {
        this.chunkSize = 1;
        logger.info("Initializing filenames in synchronized block by thread: {}", Thread.currentThread().getName());
        this.commonFilename = "mydata_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSS"));
        this.jsonFilename = this.commonFilename + ".json";
        this.jsonFileResource = new FileSystemResource(this.jsonFilename);
    }

    @Bean
    @Qualifier("myBatchJob")
    public Job myBatchJob(
            JobRepository jobRepository,
            @Qualifier("myBatchStepExport") Step stepExport) {

        return new JobBuilder("myBatchJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(stepExport)
                .build();
    }

    @Bean
    @Qualifier("myBatchStepExport")
    public Step stepExport(JobRepository jobRepository, PlatformTransactionManager transactionManager,
            @Qualifier("myBatchItemReader") SynchronizedItemStreamReader<MyData> myBatchItemReader,
            @Qualifier("myBatchJsonItemWriter") SynchronizedItemStreamWriter<MyData> myBatchJsonItemWriter,
            JdbcTemplate jdbcTemplate,
            TaskExecutor applicationTaskExecutor) {

        SimpleStepBuilder<MyData, MyData> stepBuilder = new StepBuilder("stepExport", jobRepository)
                .<MyData, MyData>chunk(chunkSize, transactionManager)
                .reader(myBatchItemReader)
                .processor(new MyDataProcessor(jdbcTemplate))
                .writer(myBatchJsonItemWriter)
                // FIXME Is this required ?
                .stream(myBatchDefaultJsonItemWriter())
                // This defines the multi-threading using the TaskExecutor
                .taskExecutor(applicationTaskExecutor)
                // Set the number of concurrent threads
                // FIXME The method is deprecated, but the replacement is not clear
                .throttleLimit(2);

        return stepBuilder.build();
    }

    /**
     * Synchronized for parallel reading (index management).
     *
     * @param dataSource
     * @param jobExecution
     * @return
     */
    @Bean
    @Qualifier("myBatchItemReader")
    @StepScope
    public SynchronizedItemStreamReader<MyData> myBatchItemReader(DataSource dataSource) {

        JdbcCursorItemReader<MyData> localReader = new JdbcCursorItemReaderBuilder<MyData>()
                .dataSource(dataSource)
                .sql(sqlQuery)
                .rowMapper(new MyDataRowMapper())
                .saveState(false)
                .build();

        // Wrap the reader with SynchronizedItemStreamReader
        SynchronizedItemStreamReader<MyData> synchronizedReader = new SynchronizedItemStreamReader<>();
        synchronizedReader.setDelegate(localReader);
        return synchronizedReader;
    }

    /**
     * Provide a synchronized item writer for JSON format.
     * 
     * https://docs.spring.io/spring-batch/reference/readers-and-writers/delegate-pattern-registering.html
     *
     * @param jsonObjectMarshaller
     * @return
     */
    @Bean
    @Qualifier("myBatchJsonItemWriter")
    @StepScope
    public SynchronizedItemStreamWriter<MyData> myBatchJsonItemWriter(
        @Qualifier("myBatchDefaultJsonItemWriter") JsonFileItemWriter<MyData> myBatchDefaultJsonItemWriter
    ) {

        // First implementation without defining bean JsonFileItemWriter<MyData> myBatchDefaultJsonItemWriter.
        //
        // JsonObjectMarshaller<MyData> marshaller = new JacksonJsonObjectMarshaller<MyData>();
        //
        // logger.info("Creating JsonFileItemWriter in synchronized block by thread: {}", Thread.currentThread().getName());
        // JsonFileItemWriter<MyData> jsonFileItemWriter = new JsonFileItemWriterBuilder<MyData>()
        //         .name("myBatchJsonItemWriter")
        //         .resource(this.jsonFileResource)
        //         .forceSync(true)
        //         .jsonObjectMarshaller(marshaller)
        //         .build();
        //
        // logger.info("JsonFileItemWriter ID: {}", System.identityHashCode(jsonFileItemWriter));
        //
        // return new SynchronizedItemStreamWriterBuilder<MyData>()
        //         .delegate(jsonFileItemWriter)
        //         .build();

        return new SynchronizedItemStreamWriterBuilder<MyData>()
                // another attempt to provide writer by calling the method instead of injecting the bean, but has no effect
                // .delegate(myBatchDefaultJsonItemWriter())
                .delegate(myBatchDefaultJsonItemWriter)
                .build();
    }

    /**
     * Provide a default JSON item writer (not thread-safe).
     * @return
     */
    @Bean
    @Qualifier("myBatchDefaultJsonItemWriter")
    @StepScope
    public JsonFileItemWriter<MyData> myBatchDefaultJsonItemWriter() {

        JsonObjectMarshaller<MyData> marshaller = new JacksonJsonObjectMarshaller<MyData>();

        return new JsonFileItemWriterBuilder<MyData>()
                .name("myBatchJsonItemWriter")
                .resource(this.jsonFileResource)
                // FIXME Set to true to enforce synchronous writing
                .forceSync(true)
                .jsonObjectMarshaller(marshaller)
                .build();

    }

    /*
     * Infrastructure beans configuration
     */

    @Bean
    public DataSource dataSource() {
        DataSource realDataSource = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                // Initialize the schema with the Spring Batch schema
                .addScript("/org/springframework/batch/core/schema-h2.sql")
                // Initialize the custom schema and data
                .addScript("/mybatch/my-schema-h2.sql")
                .build();

        return realDataSource;
    }

    @Bean
    public JdbcTransactionManager transactionManager(DataSource dataSource) {
        return new JdbcTransactionManager(dataSource);
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * Task executor for multi-threading.
     * @return
     */
    @Bean
    public TaskExecutor applicationTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(16);
        executor.setMaxPoolSize(50);
        executor.setQueueCapacity(20);
        executor.setThreadNamePrefix("Batch-");
        executor.initialize();
        return executor;
    }

    /*
     * Main method to run the application and exhibit the issue
     */
    public static void main(String[] args) throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(MyBatchJobConfiguration.class);

        JobLauncher jobLauncher = context.getBean(JobLauncher.class);
        Job job = context.getBean("myBatchJob", Job.class);
        JobParameters jobParameters = new JobParametersBuilder().addString("output", "json,xml").toJobParameters();

        JobExecution jobExecution = jobLauncher.run(job, jobParameters);
        System.out.println(jobExecution.getExitStatus().getExitCode());

        context.close(); // Ensure the context is closed to terminate the application
    }

}