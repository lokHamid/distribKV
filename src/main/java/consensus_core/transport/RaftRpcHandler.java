package consensus_core.transport;

import consensus_core.rpc.AppendEntriesArgs;
import consensus_core.rpc.AppendEntriesReply;
import consensus_core.rpc.InstallSnapshotArgs;
import consensus_core.rpc.InstallSnapshotReply;
import consensus_core.rpc.RequestVoteArgs;
import consensus_core.rpc.RequestVoteReply;

/**
 * Inbound RPC contract. A RaftNode implements this; the transport server
 * dispatches decoded requests to it and forwards the reply back to the caller.
 */
public interface RaftRpcHandler {
    RequestVoteReply handleRequestVote(RequestVoteArgs args);
    AppendEntriesReply handleAppendEntries(AppendEntriesArgs args);
    InstallSnapshotReply handleInstallSnapshot(InstallSnapshotArgs args);
}
