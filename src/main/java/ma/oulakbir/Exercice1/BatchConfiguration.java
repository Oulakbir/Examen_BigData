package ma.oulakbir.Exercice1;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
public class BatchConfiguration {

    // H2 DataSource configuration
    @Bean
    public DataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setUrl("jdbc:h2:mem:hospital-db;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        dataSource.setUsername("sa");
        dataSource.setPassword("");
        return dataSource;
    }

    @Bean
    public DataSourceTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    public FlatFileItemReader<Patient> reader() {
        return new FlatFileItemReaderBuilder<Patient>()
                .name("patientItemReader")
                .resource(new ClassPathResource("hospitalizations.csv"))
                .delimited()
                .names("id", "nom", "service", "dateAdmission", "dateSortie")
                .linesToSkip(1)
                .targetType(Patient.class)
                .build();
    }

    @Bean
    public ItemProcessor<Patient, ServiceSummary> processor() {
        return new PatientProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<ServiceSummary> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<ServiceSummary>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("MERGE INTO hospitalization_summary (service, avg_duration) KEY(service) VALUES (:service, :avgDuration)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Step step(JobRepository jobRepository, DataSourceTransactionManager transactionManager,
                     FlatFileItemReader<Patient> reader, ItemProcessor<Patient, ServiceSummary> processor,
                     JdbcBatchItemWriter<ServiceSummary> writer) {
        return new StepBuilder("step1", jobRepository)
                .<Patient, ServiceSummary>chunk(3, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public Job importPatientJob(JobRepository jobRepository, Step step, JobCompletionNotificationListener listener) {
        return new JobBuilder("importPatientJob", jobRepository)
                .listener(listener)
                .start(step)
                .build();
    }
}
