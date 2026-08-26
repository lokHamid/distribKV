// telemetry/TelemetryBus.java
package telemetry;

import consensus_core.api.RaftEvent;
import consensus_core.api.TelemetryPublisher;
import telemetry.subscriber.TelemetrySubscriber;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The concrete implementation RaftNode is handed at construction time.
 * Fans out every published event to all currently-connected subscribers
 * (dashboard sockets, metrics collector, anything else that wants to listen).
 */
public class TelemetryBus implements TelemetryPublisher {
    private final List<TelemetrySubscriber> subscribers = new CopyOnWriteArrayList<>();

    @Override
    public void publish(RaftEvent event) {
        for (TelemetrySubscriber subscriber : subscribers) {
            try {
                subscriber.onEvent(event);
            } catch (RuntimeException ignored) {
                // a dying subscriber (e.g. a socket that just closed) must never
                // break publishing to the remaining subscribers
            }
        }
    }

    public void subscribe(TelemetrySubscriber subscriber) {
        subscribers.add(subscriber);
    }

    public void unsubscribe(TelemetrySubscriber subscriber) {
        subscribers.remove(subscriber);
    }
}