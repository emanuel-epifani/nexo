# Integration / E2E Test Matrix

Every scenario has a unique ID. Both SDKs (TS, Python) must have a corresponding test.
Test names are listed as `ts:` and `py:` for easy grep matching.

---

## Store

| ID | Description | TS test | Python test |
|---|---|---|---|
| store_basic_crud | Set, get, delete keys — value matches | should perform basic CRUD operations | basic_crud |
| store_ttl_expiration | Set with TTL, verify expiry after deadline | should expire keys after TTL | ttl_expiration |
| store_ttl_zero_rejected | TTL=0 is rejected with error | should reject ttl: 0 with an error | ttl_zero_is_error |
| store_no_ttl_persistent | Key without TTL persists indefinitely | should persist keys without TTL | no_ttl_is_persistent |
| store_get_nonexistent | Get on non-existent key returns null | should return null for get on non-existent key | get_nonexistent_returns_none |
| store_del_nonexistent | Del on non-existent key is idempotent (no error) | should succeed del on non-existent key (idempotent) | del_nonexistent_is_idempotent |
| store_overwrite | Overwrite existing key with new value | should overwrite existing key with new value | overwrite_existing_key |
| store_large_value | Handle large values (1MB round-trip) | should handle large values (1MB) | large_value_1mb |
| store_incr_new_key | INCR on non-existent key starts from 0 | should increment a new key from 0 | incr_new_key_from_zero |
| store_incr_existing | INCR on existing integer value | should increment an existing integer value | incr_existing_integer |
| store_incr_negative | INCR with negative delta (decrement) | should decrement with negative delta | incr_negative_delta |
| store_incr_non_integer | INCR on non-integer value returns error | should error on non-integer value | incr_non_integer_errors |
| store_incr_preserves_ttl | INCR preserves existing TTL on key | should preserve TTL after incr | incr_preserves_ttl |
| store_incr_negative_new | INCR with negative delta on new key | should handle negative delta on new key | incr_negative_on_new_key |

## PubSub

| ID | Description | TS test | Python test |
|---|---|---|---|
| pubsub_exact_match | Publish on concrete topic, subscriber receives | should handle Exact Matches and ignore noise | exact_match_and_ignore_noise |
| pubsub_wildcard_plus | Subscribe with `+`, receive from matching single-level | should handle Single-Level Wildcard (+) with strict isolation | single_level_wildcard |
| pubsub_wildcard_hash | Subscribe with `#`, receive from matching multi-level | should handle Multi-Level Wildcard (#) correctly | multi_level_wildcard |
| pubsub_clear_retained | Publish empty payload to clear retained message | should clear retained messages | clear_retained |
| pubsub_clear_no_spurious_delivery | clearRetained does not deliver a spurious empty message to current subscribers | should not deliver a spurious message to current subscribers on clearRetained | clear_retained_does_not_deliver_to_current_subscribers |
| pubsub_reject_invalid_ttl | Invalid TTL values are rejected | should reject invalid ttl values | reject_invalid_ttl |
| pubsub_async_callbacks | Async callbacks work correctly | should support async callbacks | async_callback |
| pubsub_slow_callback_no_block | Slow callback doesn't block other operations | should not block other operations when callback is slow | slow_callback_does_not_block_store |
| pubsub_parallel_subscriptions | Parallel subscriptions on different topics execute independently | should run parallel subscriptions independently | parallel_subscriptions |
| pubsub_same_pattern_local | Two local listeners share one wildcard pattern and stop independently | should isolate two local listeners on the same pattern | two_local_listeners_share_pattern_and_stop_independently |
| pubsub_local_overflow | A bounded slow local listener stops without unbounded memory growth | should stop only the overflowing local listener | overflow_stops_only_local_listener |
| pubsub_retained_new_subscriber | Retained message delivered to new subscriber | should deliver retained message to new subscriber | retained_delivered_to_new_subscriber |
| pubsub_retained_overwrite | Second retained publish overwrites first | should overwrite retained message on second publish | retained_overwrite_on_second_publish |
| pubsub_retained_ttl_expiry | Retained with TTL not delivered after expiry | should not deliver retained message after TTL expiry | retained_not_delivered_after_ttl_expiry |
| pubsub_subscription_stop | No messages received after the owned subscription is stopped | should stop delivery after unsubscribe | unsubscribe_stops_delivery |
| pubsub_combined_wildcards | Combined wildcards `a/+/b/#` match correctly | should match combined wildcards a/+/b/# | combined_wildcards_plus_and_hash |
| pubsub_broadcast_3plus | Broadcast to 3+ subscribers on same topic | should broadcast to 3+ subscribers on same topic | broadcast_to_3_plus_subscribers |
| pubsub_disconnect_cleanup | Disconnect cleanup doesn't break topic for others | should clean up subscriber on disconnect without breaking topic | disconnect_cleanup_does_not_break_topic |
| pubsub_retained_wildcard_plus | Retained messages delivered to `+` subscriber | should deliver retained messages to wildcard + subscriber | retained_with_wildcard_plus |
| pubsub_retained_wildcard_hash | Retained messages delivered to `#` subscriber | should deliver retained messages to wildcard # subscriber | retained_with_wildcard_hash |
| pubsub_slow_consumer_disconnect | Slow consumer disconnected by server, others unaffected, auto-reconnect | should not affect other subscribers when one is disconnected by server (slow consumer) | slow_consumer_disconnect_does_not_affect_others |

## Queue

| ID | Description | TS test | Python test |
|---|---|---|---|
| queue_full_lifecycle | Create, push, subscribe, consume, ack | should handle full lifecycle: Push -> Subscribe -> Ack | full_lifecycle_push_subscribe_ack |
| queue_priority | Higher priority message consumed before lower | should respect priority (High before Low) | priority_high_before_low |
| queue_push_batch | Push batch of messages, verify all consumable | should push batch of messages | push_batch |
| queue_push_batch_mixed_priority | Batch with mixed priorities, ordering respected | should push batch with mixed priorities | push_batch_mixed_priorities |
| queue_push_batch_empty | Empty pushBatch handled gracefully | should handle empty pushBatch gracefully | empty_push_batch |
| queue_nack_dlq | Explicit NACK persists failure reason in DLQ | Should handle explicit NACK and persist failure reason in DLQ | nack_persists_failure_reason |
| queue_retry_dlq | Message exceeds max_deliveries, lands in DLQ | should move failed messages to DLQ | move_failed_to_dlq |
| queue_dlq_workflow | DLQ full workflow: peek, replay, delete, purge | Should handle DLQ workflow: peek, replay, delete, purge | dlq_workflow_peek_move_delete_purge |
| queue_concurrency_serial | concurrency=1 serializes callbacks | should serialize callbacks with concurrency=1 | serialize_callbacks_concurrency_1 |
| queue_concurrency_parallel | concurrency>1 processes messages in parallel | should process messages in parallel with concurrency > 1 | parallel_concurrency_gt_1 |
| queue_multiple_subscribers | Multiple parallel subscribers on same queue | should allow multiple parallel subscribers on the same queue (in-process scaling) | multiple_parallel_subscribers |
| queue_no_dlq_on_shutdown | Graceful shutdown requeues via visibility timeout | should not DLQ messages on graceful shutdown (requeue via visibility timeout) | no_dlq_on_graceful_shutdown |
| queue_stop_on_delete | Consumer stops when queue is deleted during subscribe | should stop consumer when queue is deleted during subscribe | stop_consumer_when_queue_deleted |
| queue_subscribe_missing | Subscribe to a non-existent queue rejects before starting a loop | should fail fast when subscribing to a missing queue | subscribe_nonexistent_queue_fails_fast |
| queue_reject_batch_size_zero | batchSize=0 in subscribe is rejected | should reject batchSize=0 in subscribe | reject_batch_size_zero |
| queue_reject_concurrency_zero | concurrency=0 in subscribe is rejected | should reject concurrency=0 in subscribe | reject_concurrency_zero |
| queue_exists | exists() returns true after create, false before | should return exists=true after create, false before | exists_true_after_create_false_before |
| queue_provisioning_result | Create returns `created`, repeated equivalent create returns `unchanged`, differing config conflicts | should return provisioning results and reject configuration conflicts | create_idempotent |
| queue_config_conflict_typed | Configuration drift raises the typed conflict error with details | should return provisioning results and reject configuration conflicts | create_config_conflict_is_typed |
| queue_get_missing | Fail-fast get on an absent resource raises typed not-found | should fail fast when getting a non-existent queue | get_nonexistent_queue_fails |
| queue_exists_transport_error | exists propagates connection errors instead of returning false | should propagate connection errors from exists | exists_propagates_connection_error |
| queue_empty_no_wait | Consume empty queue with short waitMs returns immediately | should consume empty queue without waiting and return immediately | consume_empty_queue_no_wait_returns_immediately |
| queue_partial_batch | Partial batch when fewer messages than batchSize | should return partial batch when fewer messages than batchSize | partial_batch_when_fewer_than_batch_size |
| queue_long_poll_wakeup | Long-polling consumer wakes up on push | should wake up long-polling consumer when message is pushed | long_polling_wakeup_on_push |
| queue_fifo_same_priority | FIFO ordering preserved for same-priority messages | should preserve FIFO ordering for same-priority messages | fifo_ordering_same_priority |
| queue_push_nonexistent_fails | Push to non-existent queue fails | should fail push to non-existent queue | push_nonexistent_queue_fails |
| queue_push_deleted_fails | Push to deleted queue fails | should fail push to deleted queue | push_deleted_queue_fails |
| queue_delivery_token_lifecycle | deliveryToken flows through subscribe lifecycle | should handle deliveryToken in subscribe lifecycle | delivery_token_in_subscribe_lifecycle |
| queue_redelivery_new_token | NACK triggers redelivery with new token | should requeue on stale ACK and redeliver to another consumer | redelivery_after_nack_uses_new_token |
| queue_stop_cancels_long_poll | stop() returns quickly when long-polling with no messages | stop() should return quickly when long-polling with no messages | stop_returns_quickly_when_long_polling |

## Stream

| ID | Description | TS test | Python test |
|---|---|---|---|
| stream_happy_path | Create stream, publish, fetch with consumer group | should support Happy Path (Publish/Subscribe) | happy_path_publish_subscribe |
| stream_subscribe_nonexistent | Subscribe to non-existent stream fails | should fail subscribe when stream does not exist | subscribe_nonexistent_stream |
| stream_independent_groups | Two groups each receive all messages independently | Independent CONSUMER GROUPS => should deliver all messages to each group | independent_consumer_groups |
| stream_same_group_distribution | Same group, multiple consumers, no duplicate deliveries | Same CONSUMER GROUP => should distribute messages without duplicates | same_group_no_duplicates |
| stream_consumer_disconnect | Consumer disconnect with zero data loss | should handle consumer disconnect with zero data loss | consumer_disconnect_zero_data_loss |
| stream_history_sync | New groups start from beginning (history sync) | should support History Sync (new groups start from beginning) | history_sync_new_group_from_beginning |
| stream_stop_quickly | Stop subscription quickly, not wait for long-poll timeout | should stop subscription quickly (not wait for long-poll timeout) | stop_subscription_quickly |
| stream_stop_during_callback | Stop waits for callbacks already started and their ACK responses before leaving | should commit a started callback before leaving the group | stop_commits_started_callback_before_leave |
| stream_stop_callback_timeout | Stop fails visibly instead of hanging when a callback exceeds its configured grace period | should fail stop when a started callback exceeds the stop timeout | stop_callback_timeout_is_reported |
| stream_stop_ack_failure | Batch update failure during stop is returned to the caller (fenced epoch propagates) | should expose a batch update failure during stop | stop_exposes_batch_update_failure |
| stream_active_ack_failure | Batch update failure while active triggers immediate rejoin and redelivery | should rejoin and redeliver after a batch update failure while active | active_subscription_rejoins_after_update_failure |
| stream_ordering_concurrency_1 | concurrency=1 serializes callback execution on the happy path (not a global ordering contract across keys/retries) | should preserve ordering with concurrency=1 | preserve_ordering_concurrency_1 |
| stream_parallel_concurrency | concurrency>1 processes messages in parallel | should process messages in parallel with concurrency > 1 | parallel_concurrency_gt_1 |
| stream_incremental_ack | A fast callback ACKs and frees its key without waiting for a slow callback in the same fetch batch | should ACK a fast callback without waiting for a slow callback in the same batch | fast_ack_does_not_wait_for_slow_callback_in_same_batch |
| stream_seek | Seek to beginning and end | should support Seek (Beginning/End) | seek_beginning_and_end |
| stream_stop_during_long_poll | Stop quickly during long-poll idle (no messages) | should stop quickly during long-poll wait (no messages available) | stop_quickly_during_long_poll_idle |
| stream_no_delivery_after_stop | No messages delivered after stop() returns | should not deliver messages after stop() returns | no_delivery_after_stop |
| stream_publish_batch_seq | Publish batch and return seq numbers | should publish batch and return seq numbers | publish_batch_returns_seq_numbers |
| stream_publish_batch_keys | Publish batch with keys | should publish batch with keys | publish_batch_with_keys |
| stream_publish_string_key | Publish with string key, verify receipt | should publish with string key and verify receipt | publish_with_string_key |
| stream_publish_bytes_key | Publish with Uint8Array key, verify receipt | should publish with Uint8Array key and verify receipt | publish_with_bytes_key |
| stream_publish_bytes_data | Publish Uint8Array data, receive raw bytes | should publish Uint8Array data and receive raw bytes back | publish_bytes_data |
| stream_publish_batch_empty | Empty publishBatch handled gracefully | should handle empty publishBatch gracefully | empty_publish_batch |
| stream_empty_key_rejected | Empty keys are rejected instead of silently becoming keyless | should reject empty stream keys | reject_empty_stream_keys |
| stream_publish_batch_limit | Publish batches above 65,536 items are rejected locally | should reject publish batches above the protocol limit | reject_oversized_publish_batch |
| stream_exists | exists() returns true after create, false before | should return exists=true after create, false before | exists_true_after_create_false_before |
| stream_provisioning_result | Create returns `created`, repeated equivalent create returns `unchanged`, differing config conflicts | should return provisioning results and reject configuration conflicts | create_idempotent |
| stream_config_conflict_typed | Configuration drift raises the typed conflict error with details | should return provisioning results and reject configuration conflicts | create_config_conflict_is_typed |
| stream_get_missing | Fail-fast get on an absent resource raises typed not-found | should fail fast when getting a non-existent stream | get_nonexistent_stream_fails |
| stream_exists_transport_error | exists propagates connection errors instead of returning false | should propagate connection errors from exists | exists_propagates_connection_error |
| stream_invalid_name | Stream names cannot escape the persistence directory | should reject stream names that escape the stream directory | reject_invalid_stream_name |
| stream_invalid_runtime_options | Invalid seek targets and zero polling options are rejected | should reject invalid seek and subscription polling options | reject_invalid_seek_and_subscription_options |
| stream_invalid_executor_options | Out-of-range/non-integer executor options reject before any JOIN | should reject invalid continuous-executor subscribe options before joining | reject_invalid_continuous_executor_options |
| stream_publish_nonexistent_fails | Publish to non-existent stream fails | should fail publish to non-existent stream | publish_nonexistent_stream_fails |
| stream_ops_after_delete_fail | Operations after delete fail | should fail operations after delete | operations_after_delete_fail |
| stream_peek_dls_empty | `group.dls.peek()` returns an empty array when DLS is empty | should expose group DLS peek when the DLS is empty | peek_dls_empty_returns_empty |
| stream_purge_dls_empty | `group.dls.purge()` returns 0 when DLS is empty | should expose group DLS purge when the DLS is empty | purge_dls_empty_returns_zero |
| stream_resubscribe_after_stop | Resubscribe same group after stop receives only new messages | should resubscribe same group after stop and receive only new messages | resubscribe_same_group_after_stop |
| stream_multi_groups_simultaneous | Multiple independent groups receive all messages simultaneously | should deliver messages to multiple independent groups simultaneously | multiple_groups_simultaneous_delivery |
| stream_per_key_ordering | Same-key callbacks run sequentially; different keys in parallel (not a one-message-per-wire-batch guarantee) | should enforce per-key ordering: same key delivered one at a time, different keys in parallel | per_key_ordering_same_key_one_at_a_time |
| stream_same_key_sequential | Same-key messages in one batch process strictly sequentially | should process same-key messages strictly sequentially | same_key_messages_processed_strictly_sequentially |
| stream_mid_batch_failure_prefix | Mid-batch callback failure never replays the confirmed prefix; suffix resumes after ack-wait | should not replay the confirmed prefix after a mid-batch callback failure | mid_batch_failure_does_not_replay_confirmed_prefix |
| stream_same_key_exclusivity | One in-flight lease per key — a second consumer gets nothing until the batch settles | should not lease the same key to two consumers at once | same_key_not_leased_to_two_consumers |
| stream_stop_mid_batch | Stop mid-batch confirms the processed prefix only; tail is redelivered on resubscribe | should keep the confirmed prefix on stop and redeliver only the tail | stop_mid_batch_keeps_prefix_and_redelivers_tail |
| stream_continuous_refill | Independent keys keep refilling while one batch's callback is held | should continuously refill independent keys while a batch is held | continuous_refill_independent_keys_while_batch_held |
| stream_callback_timeout | Timed-out callback is cancelled (AbortSignal/CancelledError) and other keys keep running | should abort a timed-out callback and keep serving other keys | callback_timeout_cancels_and_keeps_other_keys_running |
| stream_decode_fetch_page | Fetch-page decoder rejects malformed pages (bounds, duplicates, trailing bytes, shared storage) | decode fetch page: * | test_decode_fetch_* |
| stream_lease_renewal | Held batch renews its lease through the coalesced update channel | renews a held batch lease through the coalesced update channel | renews_held_batch_through_coalesced_updates |
| stream_fenced_batch | Per-batch FENCED kills only that batch; other keys continue without rejoin | keeps other keys running when one batch is fenced | stale_batch_fenced_without_rejoin |
| stream_epoch_rejoin | Outer FENCED rejoins under a new epoch; old receipts never update under it | rejoins under a new epoch when the session is fenced | rejoins_with_new_epoch_after_session_fence |
| stream_stop_pending_fetch | Stop during a pending long-poll leaves without waiting waitMs | leaves without waiting out the long poll when stopping during a pending fetch | stop_during_pending_fetch_leaves_without_waiting_long_poll |
| stream_buffered_credits | Never exceeds buffered message/byte/key-slot credits while callbacks are held | never exceeds buffered message and byte credits while callbacks are held | never_exceeds_buffered_credits_while_callbacks_held |
| stream_runner_slot_reserve | Runner slots are reserved before scheduling so at most `concurrency` callbacks start | reserves callback slots before starting runners | reserves_callback_slots_before_starting_runners |
| stream_fatal_leave | Fatal update outcome leaves exactly once and settles closed with a pending fetch | leaves exactly once and settles closed on a fatal update outcome with a pending fetch | leaves_once_on_fatal_update_outcome_with_pending_fetch |
| stream_cooperative_cancel | A timed-out callback that cooperates with abort/cancel does not fail the subscription | does not fail the subscription when a timed-out callback cooperates with abort | cooperative_cancel_does_not_fail_subscription |
| stream_duplicate_outcome | A duplicate outcome receipt in an update response fails the subscription | fails on a duplicate outcome receipt in an update response | fails_on_duplicate_outcome_receipt_in_update_response |
| stream_capacity_exhausted | Non-cooperative timed-out callbacks retaining all slots fail the subscription | fails the subscription when timed-out callbacks never settle | fails_subscription_when_timed_out_callbacks_never_settle |
| stream_abort_settle_race | A batch abort racing callback settlement keeps retired-capacity accounting balanced | keeps capacity accounting correct when an abort races callback settlement | capacity_stays_balanced_when_abort_races_settlement |
| stream_join_during_stop_leave | A JOIN completing during stop is never installed and its exact member identity is LEFT before stop resolves | leaves the member whose join completes while stop is in progress | leaves_member_whose_join_completes_during_stop |
| stream_bounded_cleanup_resets | Repeated session resets keep cleanup storage bounded and never reuse an old epoch token | keeps cleanup storage bounded across repeated session resets | cleanup_storage_bounded_across_repeated_resets |
| stream_join_cleanup_timeout | Stop reports a JOIN cleanup deadline failure; a late member is still left without admitting callbacks | reports an unsettled join during teardown and still leaves the late member | reports_unsettled_join_during_teardown_and_leaves_late_member |
| stream_successor_before_update_reply | A new receipt for the same key supersedes the previous assignment while its update response is still in flight | accepts a successor batch before the previous update response | accepts_successor_before_previous_update_response |
| stream_regrant_during_callback | A regrant for the same key waits behind an uncooperative prior callback; other keys keep running | waits for an uncooperative prior callback before starting a regranted key | waits_for_uncooperative_callback_before_regranted_key |
| stream_regrant_gate_epoch_reset | The local key execution gate survives an epoch reset: a fenced session's retired callback still blocks the regranted key | waits for an uncooperative prior callback before a regranted key across an epoch reset | waits_for_uncooperative_callback_before_regranted_key_across_epoch_reset |
| stream_blocked_release_on_stop | Stopping releases a regranted batch blocked behind a retired callback without starting it | releases an unstarted blocked batch during stop without starting it | releases_unstarted_blocked_batch_during_stop |
| stream_key_gate_survives_seek | A local key execution gate persists across a real seek/epoch reset while an old callback ignores cancellation; other keys progress on the new epoch | should keep a local key gate across seek while an old callback ignores cancellation | local_key_gate_survives_seek_with_uncooperative_callback |

## Connection

| ID | Description | TS test | Python test |
|---|---|---|---|
| conn_request_timeout | Request times out when server doesn't respond | should reject with RequestTimeoutError when server does not respond in time | request_timeout |
| conn_fire_and_forget_disconnected | Fire-and-forget silently dropped when disconnected | sendFireAndForget should silently return when disconnected (no throw) | fire_and_forget_when_disconnected |
| conn_reject_pending_on_disconnect | Pending requests rejected on disconnect | should reject pending requests on disconnect | reject_pending_requests_on_disconnect |
| conn_signal_listeners | SIGINT/SIGTERM listeners registered and removed | should register and remove SIGINT/SIGTERM listeners per client | signal_listeners_registered_and_removed |
| conn_multiple_clients_listeners | Multiple clients register listeners independently | should register listeners for multiple clients independently | multiple_clients_independent_listeners |

## Reconnection

| ID | Description | TS test | Python test |
|---|---|---|---|
| conn_pubsub_auto_resubscribe | After reconnect, subscriptions are restored | PUBSUB: Should auto-resubscribe after connection loss | pubsub_auto_resubscribe |
| conn_queue_resume_consume | After reconnect, consumer loop resumes | QUEUE: Should resume consuming after connection loss | queue_resume_consuming |
| conn_stream_rejoin_group | After reconnect, consumer rejoins group | STREAM: Should resume consuming after connection loss (Rejoin Group) | stream_resume_consuming |
| conn_queue_inflight_redelivered | In-flight message redelivered after crash (at-least-once) | QUEUE: In-flight message should be redelivered after crash (at-least-once) | queue_inflight_redelivered |
| conn_queue_push_during_disconnect | push() during disconnect fails with predictable error | QUEUE: push() during disconnect should fail with predictable error | queue_push_during_disconnect_fails |
| conn_queue_double_crash | Survive double crash without duplicating consumer loops | QUEUE: Should survive double crash without duplicating consumer loops | queue_survive_double_crash |
| conn_queue_stop_during_disconnect | stop() during disconnect prevents loop from resuming | QUEUE: stop() during disconnect should prevent loop from resuming | queue_stop_during_disconnect |
| conn_pubsub_topics_before_after_crash | Receive messages on topics subscribed before AND after crash | PUBSUB: Should receive messages on topics subscribed before AND after crash | pubsub_topics_before_and_after_crash |
| conn_stream_inflight_redelivered | In-flight stream message redelivered after crash | STREAM: In-flight message should be redelivered after crash (at-least-once) | stream_inflight_redelivered |
| conn_stream_same_key_serial | Same-key messages delivered serially via TCP | STREAM: Same-key messages should be delivered serially via TCP | stream_same_key_serial_ordering |

## Cross-Broker

| ID | Description | TS test | Python test |
|---|---|---|---|
| cross_store_binary | Store and retrieve raw Buffer/bytes | STORE: Should store and retrieve raw Buffer | store_binary_payload |
| cross_queue_binary | Push and pop raw Buffer/bytes | QUEUE: Should push and pop raw Buffer | queue_binary_payload |
| cross_pubsub_binary | Publish and subscribe raw Buffer/bytes | PUBSUB: Should publish and subscribe raw Buffer | pubsub_binary_payload |
| cross_stream_binary | Stream raw Buffer/bytes | STREAM: Should stream raw Buffer | stream_binary_payload |
| cross_json_special_chars | JSON serialization with special chars and nested objects | should handle JSON serialization with special chars and nested objects | json_special_chars_and_nested |
| cross_empty_string_vs_null | Distinguish between empty string and null | should distinguish between empty string and null | distinguish_empty_string_and_null |
