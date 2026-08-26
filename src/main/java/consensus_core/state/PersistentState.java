package consensus_core.state;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Persists Raft's durable state (currentTerm + votedFor) to disk so a node can
 * honour the "at most one vote per term" and "never vote in an older term"
 * invariants across a restart. All mutations are flushed synchronously.
 */
public class PersistentState {
    private long currentTerm = 0;
    private String votedFor = null;
    private FileChannel fileChannel;
    private final Path storageFilePath;

    public PersistentState(Path storageFilePath) {
        this.storageFilePath = storageFilePath;
    }

    /** Opens/creates the storage file and loads any previously persisted state. */
    public synchronized void load() {
        try {
            if (storageFilePath.getParent() != null) {
                java.nio.file.Files.createDirectories(storageFilePath.getParent());
            }
            fileChannel = FileChannel.open(
                    storageFilePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            long size = fileChannel.size();
            if (size > 0) {
                ByteBuffer buf = ByteBuffer.allocate((int) size);
                fileChannel.read(buf, 0);
                buf.flip();
                currentTerm = buf.getLong();
                int len = buf.getInt();
                byte[] votedForBytes = new byte[len];
                buf.get(votedForBytes);
                votedFor = len == 0 ? null : new String(votedForBytes, StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load persistent state from " + storageFilePath, e);
        }
    }

    private synchronized void persist() {
        if (fileChannel == null) {
            return;
        }
        byte[] votedForBytes = (votedFor == null)
                ? new byte[0]
                : votedFor.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(8 + 4 + votedForBytes.length);
        buf.putLong(currentTerm);
        buf.putInt(votedForBytes.length);
        buf.put(votedForBytes);
        buf.flip();
        try {
            fileChannel.truncate(0);
            fileChannel.position(0);
            while (buf.hasRemaining()) {
                fileChannel.write(buf);
            }
            fileChannel.force(true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist state to " + storageFilePath, e);
        }
    }

    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    public synchronized void setCurrentTerm(long currentTerm) {
        if (currentTerm > this.currentTerm) {
            this.currentTerm = currentTerm;
            persist();
        }
    }

    public synchronized String getVotedFor() {
        return votedFor;
    }

    public synchronized void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
        persist();
    }

    public synchronized void close() {
        if (fileChannel != null) {
            try {
                fileChannel.close();
            } catch (IOException ignored) {
                // best effort
            }
            fileChannel = null;
        }
    }
}
