package consensus_core.api;
public interface TelemetryPublisher {
    void publish(RaftEvent event);
}