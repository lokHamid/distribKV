// consensus_core/api/EventType.java
package consensus_core.api;

public enum EventType {
    ROLE_CHANGED,       // metadata: {"from": "FOLLOWER", "to": "CANDIDATE"}
    TERM_CHANGED,
    VOTE_GRANTED,       // metadata: {"votedFor": "node-2"}
    VOTE_DENIED,
    LEADER_ELECTED,     // metadata: {"leaderId": "node-3"}
    LEADER_LOST,        // this node stopped hearing from a leader / stepped down
    ENTRY_APPENDED,     // metadata: {"index": "42"}
    ENTRY_COMMITTED,    // metadata: {"index": "42"}
    HEARTBEAT_SENT,
    HEARTBEAT_RECEIVED,
    NODE_STARTED,
    // chaos-harness events (network fault injection)
    CHAOS_PARTITIONED,       // metadata: {"groups": "node-1:0,node-2:1"}
    CHAOS_HEALED,            // partition healed
    CHAOS_NODE_KILLED,       // metadata: {"nodeId": "node-2"}
    CHAOS_NODE_RESTARTED,    // metadata: {"nodeId": "node-2"}
    CHAOS_MESSAGE_DROPPED,   // metadata: {"peer": "node-2", "reason": "partition"|"drop"}
    CHAOS_MESSAGE_DELAYED    // metadata: {"peer": "node-2", "delayMillis": "500"}
}