package consensus_core.api;

/**
 * Optional capability for a {@link StateMachine} to produce a full snapshot of
 * its current state as a byte payload, so a leader can compact its log.
 */
public interface SnapshotProvider {
    byte[] createSnapshot();
}
