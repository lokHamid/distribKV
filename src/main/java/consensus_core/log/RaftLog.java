package consensus_core.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A Raft log with in-memory entries and durable append/truncate support.
 *
 * <p>Entries are physically indexed starting at {@code lastSnapshotIndex + 1};
 * the list offsets are logical but each stored LogEntry carries its absolute
 * index. Log indices are 1-based (index 0 is the sentinel snapshot position).
 */
public class RaftLog {
    private final List<LogEntry> entries = new ArrayList<>();
    private long lastSnapshotIndex = 0;
    private long lastSnapshotTerm = 0;
    private FileChannel logFileChannel;
    private final Path logFilePath;

    public RaftLog(Path logFilePath) {
        this.logFilePath = logFilePath;
    }

    /** Opens/creates the log file and replays any persisted entries. */
    public synchronized void load() {
        try {
            if (logFilePath.getParent() != null) {
                Files.createDirectories(logFilePath.getParent());
            }
            logFileChannel = FileChannel.open(
                    logFilePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            long size = logFileChannel.size();
            if (size > 0) {
                ByteBuffer buf = ByteBuffer.allocate((int) size);
                logFileChannel.read(buf, 0);
                buf.flip();
                // header: [snapshotIndex:8][snapshotTerm:8]
                lastSnapshotIndex = buf.getLong();
                lastSnapshotTerm = buf.getLong();
                while (buf.remaining() >= 8 + 8 + 4) {
                    long index = buf.getLong();
                    long term = buf.getLong();
                    int len = buf.getInt();
                    if (buf.remaining() < len) {
                        break; // incomplete trailing frame — treat as crash remnant
                    }
                    byte[] command = new byte[len];
                    buf.get(command);
                    entries.add(new LogEntry(index, term, command));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load log from " + logFilePath, e);
        }
    }

    public synchronized long getLastSnapshotIndex() { return lastSnapshotIndex; }
    public synchronized long getLastSnapshotTerm() { return lastSnapshotTerm; }

    public synchronized long getLastIndex() {
        return entries.isEmpty() ? lastSnapshotIndex : entries.get(entries.size() - 1).getIndex();
    }

    public synchronized long getLastTerm() {
        return entries.isEmpty() ? lastSnapshotTerm : entries.get(entries.size() - 1).getTerm();
    }

    public synchronized boolean containsIndex(long index) {
        return index > lastSnapshotIndex && index <= getLastIndex();
    }

    public synchronized LogEntry getEntryAt(long index) {
        if (!containsIndex(index)) {
            return null;
        }
        return entries.get((int) (index - lastSnapshotIndex - 1));
    }

    public synchronized long getTermAt(long index) {
        if (index == lastSnapshotIndex) {
            return lastSnapshotTerm;
        }
        LogEntry entry = getEntryAt(index);
        return entry == null ? -1 : entry.getTerm();
    }

    /** Returns a defensive copy of all entries after {@code fromIndex} (inclusive), in order. */
    public synchronized List<LogEntry> getEntriesFrom(long fromIndex) {
        if (fromIndex <= lastSnapshotIndex) {
            return Collections.unmodifiableList(new ArrayList<>(entries));
        }
        int start = (int) (fromIndex - lastSnapshotIndex - 1);
        if (start < 0 || start >= entries.size()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(entries.subList(start, entries.size())));
    }

    /** Returns a defensive copy of every entry currently held in memory. */
    public synchronized List<LogEntry> snapshotView() {
        return Collections.unmodifiableList(new ArrayList<>(entries));
    }

    /** Appends a single entry. Will fail hard if the index is not contiguous. */
    public synchronized void append(LogEntry entry) {
        if (getLastIndex() != entry.getIndex() - 1) {
            throw new IllegalStateException(
                    "Expected next index " + (getLastIndex() + 1) + " but got " + entry.getIndex());
        }
        entries.add(entry);
        persist();
    }

    /** Appends an ordered batch (from AppendEntries). */
    public synchronized void append(List<LogEntry> batch) {
        for (LogEntry entry : batch) {
            if (entry.getIndex() != getLastIndex() + 1) {
                throw new IllegalStateException(
                        "Expected next index " + (getLastIndex() + 1) + " but got " + entry.getIndex());
            }
            entries.add(entry);
        }
        if (!batch.isEmpty()) {
            persist();
        }
    }

    /** Removes all entries with index {@code > endIndex} (conflict resolution). */
    public synchronized void truncateSuffix(long endIndex) {
        boolean removed = false;
        while (!entries.isEmpty() && entries.get(entries.size() - 1).getIndex() > endIndex) {
            entries.remove(entries.size() - 1);
            removed = true;
        }
        if (removed) {
            persist();
        }
    }

    /** Drops in-memory entries covered by a freshly installed snapshot. */
    public synchronized void applySnapshot(long lastIncludedIndex, long lastIncludedTerm) {
        entries.clear();
        lastSnapshotIndex = lastIncludedIndex;
        lastSnapshotTerm = lastIncludedTerm;
        persist();
    }

    private synchronized void persist() {
        if (logFileChannel == null) {
            return;
        }
        int size = 16; // header
        for (LogEntry entry : entries) {
            size += 8 + 8 + 4 + entry.getCommand().length;
        }
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putLong(lastSnapshotIndex);
        buf.putLong(lastSnapshotTerm);
        for (LogEntry entry : entries) {
            buf.putLong(entry.getIndex());
            buf.putLong(entry.getTerm());
            buf.putInt(entry.getCommand().length);
            buf.put(entry.getCommand());
        }
        buf.flip();
        try {
            logFileChannel.truncate(0);
            logFileChannel.position(0);
            while (buf.hasRemaining()) {
                logFileChannel.write(buf);
            }
            logFileChannel.force(true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist log to " + logFilePath, e);
        }
    }

    public synchronized void close() {
        if (logFileChannel != null) {
            try {
                logFileChannel.close();
            } catch (IOException ignored) {
                // best effort
            }
            logFileChannel = null;
        }
    }
}
