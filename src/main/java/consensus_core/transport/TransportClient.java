package consensus_core.transport;

import consensus_core.rpc.AppendEntriesArgs;
import consensus_core.rpc.AppendEntriesReply;
import consensus_core.rpc.InstallSnapshotArgs;
import consensus_core.rpc.InstallSnapshotReply;
import consensus_core.rpc.RequestVoteArgs;
import consensus_core.rpc.RequestVoteReply;

import java.util.concurrent.CompletableFuture;

/**
 * Outbound transport used by RaftNode to reach peer nodes. Incoming RPCs are
 * delivered to a {@link RaftRpcHandler} by the corresponding server component.
 */
public interface TransportClient {
    CompletableFuture<RequestVoteReply> sendRequestVote(String targetNodeId, RequestVoteArgs args);
    CompletableFuture<AppendEntriesReply> sendAppendEntries(String targetNodeId, AppendEntriesArgs args);
    CompletableFuture<InstallSnapshotReply> sendInstallSnapshot(String targetNodeId, InstallSnapshotArgs args);
}
