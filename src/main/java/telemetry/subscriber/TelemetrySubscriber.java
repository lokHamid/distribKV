package telemetry.subscriber;

import consensus_core.api.RaftEvent;

public interface TelemetrySubscriber {
    void onEvent(RaftEvent event);
}