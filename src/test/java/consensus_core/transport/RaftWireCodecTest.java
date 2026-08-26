package consensus_core.transport;

import consensus_core.log.LogEntry;
import consensus_core.rpc.AppendEntriesArgs;
import consensus_core.rpc.AppendEntriesReply;
import consensus_core.rpc.InstallSnapshotArgs;
import consensus_core.rpc.InstallSnapshotReply;
import consensus_core.rpc.RequestVoteArgs;
import consensus_core.rpc.RequestVoteReply;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class RaftWireCodecTest {

    private static final long CORR = 42L;

    @Test
    void requestVoteFrameRoundTrips() {
        byte[] frame = RaftWireCodec.requestVoteFrame(CORR,
                new RequestVoteArgs(3, "candidate-1", 10, 2));
        RaftWireCodec.Frame parsed = RaftWireCodec.parseFrame(frame);
        assertEquals(RaftWireCodec.TYPE_REQUEST_VOTE, parsed.type());
        assertEquals(CORR, parsed.correlationId());
        RequestVoteArgs decoded = RaftWireCodec.decodeRequestVote(parsed.payload());
        assertEquals(3, decoded.getTerm());
        assertEquals("candidate-1", decoded.getCandidateId());
        assertEquals(10, decoded.getLastLogIndex());
        assertEquals(2, decoded.getLastLogTerm());
    }

    @Test
    void requestVoteReplyFrameRoundTrips() {
        byte[] frame = RaftWireCodec.requestVoteReplyFrame(CORR, new RequestVoteReply(3, true));
        RaftWireCodec.Frame parsed = RaftWireCodec.parseFrame(frame);
        RequestVoteReply decoded = RaftWireCodec.decodeRequestVoteReply(parsed.payload());
        assertEquals(3, decoded.getTerm());
        assertTrue(decoded.isVoteGranted());
    }

    @Test
    void appendEntriesFrameRoundTrips() {
        AppendEntriesArgs args = new AppendEntriesArgs(5, "leader-9", 2, 1,
                List.of(new LogEntry(3, 1, "a".getBytes()), new LogEntry(4, 2, "bb".getBytes())), 4);
        byte[] frame = RaftWireCodec.appendEntriesFrame(CORR, args);
        RaftWireCodec.Frame parsed = RaftWireCodec.parseFrame(frame);
        assertEquals(RaftWireCodec.TYPE_APPEND_ENTRIES, parsed.type());
        AppendEntriesArgs decoded = RaftWireCodec.decodeAppendEntries(parsed.payload());
        assertEquals(5, decoded.getTerm());
        assertEquals("leader-9", decoded.getLeaderId());
        assertEquals(2, decoded.getPrevLogIndex());
        assertEquals(1, decoded.getPrevLogTerm());
        assertEquals(4, decoded.getLeaderCommit());
        assertEquals(2, decoded.getEntries().size());
        assertEquals(3, decoded.getEntries().get(0).getIndex());
        assertArrayEquals("bb".getBytes(), decoded.getEntries().get(1).getCommand());
    }

    @Test
    void appendEntriesReplyFrameRoundTrips() {
        byte[] frame = RaftWireCodec.appendEntriesReplyFrame(CORR, new AppendEntriesReply(5, true, 4));
        AppendEntriesReply decoded = RaftWireCodec.decodeAppendEntriesReply(
                RaftWireCodec.parseFrame(frame).payload());
        assertEquals(5, decoded.getTerm());
        assertTrue(decoded.isSuccess());
        assertEquals(4, decoded.getMatchIndex());
    }

    @Test
    void installSnapshotFrameRoundTrips() {
        byte[] snapshot = {1, 2, 3, 4, 5};
        byte[] frame = RaftWireCodec.installSnapshotFrame(CORR,
                new InstallSnapshotArgs(6, "leader-9", 100, 7, snapshot));
        RaftWireCodec.Frame parsed = RaftWireCodec.parseFrame(frame);
        assertEquals(RaftWireCodec.TYPE_INSTALL_SNAPSHOT, parsed.type());
        InstallSnapshotArgs decoded = RaftWireCodec.decodeInstallSnapshot(parsed.payload());
        assertEquals(6, decoded.getTerm());
        assertEquals("leader-9", decoded.getLeaderId());
        assertEquals(100, decoded.getLastIncludedIndex());
        assertEquals(7, decoded.getLastIncludedTerm());
        assertArrayEquals(snapshot, decoded.getData());
    }

    @Test
    void installSnapshotReplyFrameRoundTrips() {
        byte[] frame = RaftWireCodec.installSnapshotReplyFrame(CORR, new InstallSnapshotReply(6, false));
        InstallSnapshotReply decoded = RaftWireCodec.decodeInstallSnapshotReply(
                RaftWireCodec.parseFrame(frame).payload());
        assertEquals(6, decoded.getTerm());
        assertFalse(decoded.isSuccess());
    }
}
