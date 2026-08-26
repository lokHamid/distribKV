package consensus_core.transport;

import consensus_core.log.LogEntry;
import consensus_core.rpc.AppendEntriesArgs;
import consensus_core.rpc.AppendEntriesReply;
import consensus_core.rpc.InstallSnapshotArgs;
import consensus_core.rpc.InstallSnapshotReply;
import consensus_core.rpc.RequestVoteArgs;
import consensus_core.rpc.RequestVoteReply;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Binary framing + payload codec for Raft RPCs over WebSocket.
 *
 * <p>Frame layout: [type:1][correlationId:8][payloadLen:4][payload]. The payload
 * layout is specific to each RPC type (see the private encode/decode helpers).
 */
public final class RaftWireCodec {

    public static final byte TYPE_REQUEST_VOTE = 0;
    public static final byte TYPE_REQUEST_VOTE_REPLY = 1;
    public static final byte TYPE_APPEND_ENTRIES = 2;
    public static final byte TYPE_APPEND_ENTRIES_REPLY = 3;
    public static final byte TYPE_INSTALL_SNAPSHOT = 4;
    public static final byte TYPE_INSTALL_SNAPSHOT_REPLY = 5;

    private RaftWireCodec() {
    }

    public static byte[] requestVoteFrame(long correlationId, RequestVoteArgs args) {
        ByteBuffer p = ByteBuffer.allocate(8 + 4 + bytes(args.getCandidateId()).length + 8 + 8);
        p.putLong(args.getTerm());
        putString(p, args.getCandidateId());
        p.putLong(args.getLastLogIndex());
        p.putLong(args.getLastLogTerm());
        return frame(TYPE_REQUEST_VOTE, correlationId, p.array());
    }

    public static RequestVoteArgs decodeRequestVote(byte[] payload) {
        ByteBuffer p = ByteBuffer.wrap(payload);
        long term = p.getLong();
        String candidateId = getString(p);
        long lastLogIndex = p.getLong();
        long lastLogTerm = p.getLong();
        return new RequestVoteArgs(term, candidateId, lastLogIndex, lastLogTerm);
    }

    public static byte[] requestVoteReplyFrame(long correlationId, RequestVoteReply reply) {
        ByteBuffer p = ByteBuffer.allocate(8 + 1);
        p.putLong(reply.getTerm());
        p.put((byte) (reply.isVoteGranted() ? 1 : 0));
        return frame(TYPE_REQUEST_VOTE_REPLY, correlationId, p.array());
    }

    public static RequestVoteReply decodeRequestVoteReply(byte[] payload) {
        ByteBuffer p = ByteBuffer.wrap(payload);
        long term = p.getLong();
        boolean granted = p.get() == 1;
        return new RequestVoteReply(term, granted);
    }

    public static byte[] appendEntriesFrame(long correlationId, AppendEntriesArgs args) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeLong(args.getTerm());
            writeString(out, args.getLeaderId());
            out.writeLong(args.getPrevLogIndex());
            out.writeLong(args.getPrevLogTerm());
            out.writeLong(args.getLeaderCommit());
            out.writeInt(args.getEntries().size());
            for (LogEntry e : args.getEntries()) {
                out.writeLong(e.getIndex());
                out.writeLong(e.getTerm());
                out.writeInt(e.getCommand().length);
                out.write(e.getCommand());
            }
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
        return frame(TYPE_APPEND_ENTRIES, correlationId, bos.toByteArray());
    }

    public static AppendEntriesArgs decodeAppendEntries(byte[] payload) {
        ByteBuffer p = ByteBuffer.wrap(payload);
        long term = p.getLong();
        String leaderId = getString(p);
        long prevLogIndex = p.getLong();
        long prevLogTerm = p.getLong();
        long leaderCommit = p.getLong();
        int count = p.getInt();
        List<LogEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long index = p.getLong();
            long eterm = p.getLong();
            int len = p.getInt();
            byte[] cmd = new byte[len];
            p.get(cmd);
            entries.add(new LogEntry(index, eterm, cmd));
        }
        return new AppendEntriesArgs(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    public static byte[] appendEntriesReplyFrame(long correlationId, AppendEntriesReply reply) {
        ByteBuffer p = ByteBuffer.allocate(8 + 1 + 8);
        p.putLong(reply.getTerm());
        p.put((byte) (reply.isSuccess() ? 1 : 0));
        p.putLong(reply.getMatchIndex());
        return frame(TYPE_APPEND_ENTRIES_REPLY, correlationId, p.array());
    }

    public static AppendEntriesReply decodeAppendEntriesReply(byte[] payload) {
        ByteBuffer p = ByteBuffer.wrap(payload);
        long term = p.getLong();
        boolean success = p.get() == 1;
        long matchIndex = p.getLong();
        return new AppendEntriesReply(term, success, matchIndex);
    }

    public static byte[] installSnapshotFrame(long correlationId, InstallSnapshotArgs args) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeLong(args.getTerm());
            writeString(out, args.getLeaderId());
            out.writeLong(args.getLastIncludedIndex());
            out.writeLong(args.getLastIncludedTerm());
            out.writeInt(args.getData().length);
            out.write(args.getData());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
        return frame(TYPE_INSTALL_SNAPSHOT, correlationId, bos.toByteArray());
    }

    public static InstallSnapshotArgs decodeInstallSnapshot(byte[] payload) {
        ByteBuffer p = ByteBuffer.wrap(payload);
        long term = p.getLong();
        String leaderId = getString(p);
        long lastIncludedIndex = p.getLong();
        long lastIncludedTerm = p.getLong();
        int len = p.getInt();
        byte[] data = new byte[len];
        p.get(data);
        return new InstallSnapshotArgs(term, leaderId, lastIncludedIndex, lastIncludedTerm, data);
    }

    public static byte[] installSnapshotReplyFrame(long correlationId, InstallSnapshotReply reply) {
        ByteBuffer p = ByteBuffer.allocate(8 + 1);
        p.putLong(reply.getTerm());
        p.put((byte) (reply.isSuccess() ? 1 : 0));
        return frame(TYPE_INSTALL_SNAPSHOT_REPLY, correlationId, p.array());
    }

    public static InstallSnapshotReply decodeInstallSnapshotReply(byte[] payload) {
        ByteBuffer p = ByteBuffer.wrap(payload);
        long term = p.getLong();
        boolean success = p.get() == 1;
        return new InstallSnapshotReply(term, success);
    }

    /** Splits a raw frame into its parts. The returned payload is a fresh copy. */
    public static Frame parseFrame(byte[] frame) {
        ByteBuffer p = ByteBuffer.wrap(frame);
        byte type = p.get();
        long correlationId = p.getLong();
        int len = p.getInt();
        byte[] payload = new byte[len];
        p.get(payload);
        return new Frame(type, correlationId, payload);
    }

    private static byte[] frame(byte type, long correlationId, byte[] payload) {
        ByteBuffer f = ByteBuffer.allocate(1 + 8 + 4 + payload.length);
        f.put(type);
        f.putLong(correlationId);
        f.putInt(payload.length);
        f.put(payload);
        return f.array();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void putString(ByteBuffer p, String s) {
        byte[] b = bytes(s);
        p.putInt(b.length);
        p.put(b);
    }

    private static String getString(ByteBuffer p) {
        int len = p.getInt();
        byte[] b = new byte[len];
        p.get(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = bytes(s);
        out.writeInt(b.length);
        out.write(b);
    }

    /** Immutable decoded frame. */
    public record Frame(byte type, long correlationId, byte[] payload) {
    }
}
