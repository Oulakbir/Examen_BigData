package ma.oulakbir.Exercice2;


public class TimeTracker {
    // Attributes for storing total duration, count, and last timestamp
    public long totalDuration;  // Total time between incidents
    public long count;          // Number of incidents
    public long lastTimestamp;  // Timestamp of the last incident
    public String service;      // The service for which we are tracking incidents

    // Constructor to initialize the attributes
    public TimeTracker(long totalDuration, long count, long lastTimestamp, String service) {
        this.totalDuration = totalDuration;
        this.count = count;
        this.lastTimestamp = lastTimestamp;
        this.service = service;
    }

    // Setter methods if needed (optional)
    public void setTotalDuration(long totalDuration) {
        this.totalDuration = totalDuration;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void setLastTimestamp(long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }

    public void setService(String service) {
        this.service = service;
    }

    // Getter methods (optional)
    public long getTotalDuration() {
        return totalDuration;
    }

    public long getCount() {
        return count;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public String getService() {
        return service;
    }
}

