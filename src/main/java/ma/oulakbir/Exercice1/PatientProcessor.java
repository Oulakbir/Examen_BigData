package ma.oulakbir.Exercice1;

import org.springframework.batch.item.ItemProcessor;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PatientProcessor implements ItemProcessor<Patient, ServiceSummary> {

    private final Map<String, List<Long>> serviceDurations = new HashMap<>();

    @Override
    public ServiceSummary process(Patient patient) throws Exception {
        if (patient.dateSortie() == null || patient.dateSortie().isEmpty()) {
            return null; // Skip records with missing DateSortie
        }

        // Calculate stay duration
        LocalDate admissionDate = LocalDate.parse(patient.dateAdmission());
        LocalDate dischargeDate = LocalDate.parse(patient.dateSortie());
        long duration = ChronoUnit.DAYS.between(admissionDate, dischargeDate);

        // Aggregate durations by service
        serviceDurations.computeIfAbsent(patient.service(), k -> new ArrayList<>()).add(duration);

        // Calculate average duration for the service
        double avgDuration = serviceDurations.get(patient.service()).stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0);

        return new ServiceSummary(patient.service(), avgDuration);
    }
}

