package ma.oulakbir.Exercice1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

    private static final Logger logger = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final JdbcTemplate jdbcTemplate;

    public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        logger.info("Job completed. Fetching summary from the database...");

        List<ServiceSummary> summaries = jdbcTemplate.query(
                "SELECT service, avg_duration FROM hospitalization_summary",
                new DataClassRowMapper<>(ServiceSummary.class));

        logger.info("Average durations by service:");
        summaries.forEach(summary -> logger.info("Service: {}, Average Duration: {}", summary.service(), summary.avgDuration()));
    }
}
