package consensus_core.log;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {

    @TempDir
    Path tempDir;

    @Test
    void appendAndReadIndexes() {
        RaftLog log = new RaftLog(tempDir.resolve("log.bin"));
        log.append(new LogEntry(1, 1, "one".getBytes()));
        log.append(new LogEntry(2, 1, "two".getBytes()));
        log.append(new LogEntry(3, 2, "three".getBytes()));

        assertEquals(3, log.getLastIndex());
        assertEquals(2, log.getLastTerm());
        assertEquals(1, log.getTermAt(1));
        assertEquals(2, log.getTermAt(3));
        assertArrayEquals("two".getBytes(), log.getEntryAt(2).getCommand());
        assertNull(log.getEntryAt(99));
    }

    @Test
    void entriesFromReturnsSuffix() {
        RaftLog log = new RaftLog(tempDir.resolve("log.bin"));
        log.append(new LogEntry(1, 1, new byte[]{1}));
        log.append(new LogEntry(2, 1, new byte[]{2}));
        log.append(new LogEntry(3, 1, new byte[]{3}));
        var suffix = log.getEntriesFrom(2);
        assertEquals(2, suffix.size());
        assertEquals(2, suffix.get(0).getIndex());
        assertEquals(3, suffix.get(1).getIndex());
    }

    @Test
    void truncateRemovesSuffixPreservingContiguity() {
        RaftLog log = new RaftLog(tempDir.resolve("log.bin"));
        log.append(new LogEntry(1, 1, new byte[]{1}));
        log.append(new LogEntry(2, 1, new byte[]{2}));
        log.append(new LogEntry(3, 1, new byte[]{3}));
        log.truncateSuffix(1);
        assertEquals(1, log.getLastIndex());
        log.append(new LogEntry(2, 2, new byte[]{99}));
        assertEquals(2, log.getLastIndex());
        assertEquals(2, log.getTermAt(2));
    }

    @Test
    void persistenceSurvivesReopen() {
        Path file = tempDir.resolve("log.bin");
        RaftLog log = new RaftLog(file);
        log.load();
        log.append(new LogEntry(1, 1, "alpha".getBytes()));
        log.append(new LogEntry(2, 2, "beta".getBytes()));
        log.close();

        RaftLog reopened = new RaftLog(file);
        reopened.load();
        assertEquals(2, reopened.getLastIndex());
        assertEquals(2, reopened.getLastTerm());
        assertArrayEquals("alpha".getBytes(), reopened.getEntryAt(1).getCommand());
        assertArrayEquals("beta".getBytes(), reopened.getEntryAt(2).getCommand());
        reopened.close();
    }

    @Test
    void snapshotCompactionDropsOldEntries() {
        RaftLog log = new RaftLog(tempDir.resolve("log.bin"));
        log.append(new LogEntry(1, 1, new byte[]{1}));
        log.append(new LogEntry(2, 1, new byte[]{2}));
        log.append(new LogEntry(3, 1, new byte[]{3}));
        log.applySnapshot(3, 1);
        assertEquals(3, log.getLastIndex());
        assertEquals(1, log.getTermAt(3));
        assertNull(log.getEntryAt(3)); // compacted out of memory
        assertTrue(log.getEntriesFrom(1).isEmpty());
    }
}
