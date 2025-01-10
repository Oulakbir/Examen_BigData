package ma.oulakbir.Exercice1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class PatientJobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(PatientJobScheduler.class);

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job importPatientJob;

    @Scheduled(cron = "0 0 0 * * *") // Runs daily at midnight
    public void runJob() {
        try {
            JobParameters parameters = new JobParametersBuilder()
                    .addLong("startTime", System.currentTimeMillis())
                    .toJobParameters();

            jobLauncher.run(importPatientJob, parameters);
            logger.info("Batch job successfully launched.");
        } catch (Exception e) {
            logger.error("Failed to execute batch job.", e);
        }
    }
}
