package consensus_core.state;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class PersistentStateTest {

    @TempDir
    Path tempDir;

    @Test
    void termPersistenceSurvivesReopen() {
        Path file = tempDir.resolve("persist.bin");
        PersistentState state = new PersistentState(file);
        state.load();
        assertEquals(0, state.getCurrentTerm());
        state.setCurrentTerm(3);
        state.setVotedFor("node-2");
        state.close();

        PersistentState reopened = new PersistentState(file);
        reopened.load();
        assertEquals(3, reopened.getCurrentTerm());
        assertEquals("node-2", reopened.getVotedFor());
        reopened.close();
    }

    @Test
    void termNeverRegresses() {
        PersistentState state = new PersistentState(tempDir.resolve("p2.bin"));
        state.load();
        state.setCurrentTerm(5);
        state.setCurrentTerm(2); // ignored
        assertEquals(5, state.getCurrentTerm());
        state.close();
    }

    @Test
    void votedForIsClearedable() {
        PersistentState state = new PersistentState(tempDir.resolve("p3.bin"));
        state.load();
        state.setVotedFor("x");
        state.setVotedFor(null);
        assertNull(state.getVotedFor());
        state.close();
    }
}
