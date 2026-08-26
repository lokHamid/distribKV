package consensus_core.api;

public interface StateMachine {
    /**
     * Applies a committed command byte payload to the state machine.
     * @param command raw byte array executed against memory/storage
     * @return byte[] result returned back to client caller
     */
    byte[] apply(byte[] command);

    /**
     * Restores state machine state from a snapshot byte stream.
     */
    void installSnapshot(byte[] snapshotData);
}
