use lightning::util::persist::{KVStore, read_channel_monitors};
use lightning::ln::functional_test_utils::*;
use lightning::chain::channelmonitor::CLOSED_CHANNEL_UPDATE_ID;
use lightning::util::test_utils;
use lightning::{check_closed_broadcast, check_closed_event, check_added_monitors};
use lightning::events::ClosureReason;

pub(crate) fn do_read_write_remove_list_persist<K: KVStore>(kv_store: &K) {
	use lightning::util::ser::Readable;

	let data = [42u8; 32];

	let namespace = "testspace";
	let key = "testkey";

	// Test the basic KVStore operations.
	kv_store.write(namespace, key, &data).unwrap();

	// Test empty namespace is allowed, but not empty key.
	kv_store.write("", key, &data).unwrap();
	assert!(kv_store.write(namespace, "", &data).is_err());

	let listed_keys = kv_store.list(namespace).unwrap();
	assert_eq!(listed_keys.len(), 1);
	assert_eq!(listed_keys[0], key);

	let mut reader = kv_store.read(namespace, key).unwrap();
	let read_data: [u8; 32] = Readable::read(&mut reader).unwrap();
	assert_eq!(data, read_data);

	kv_store.remove(namespace, key).unwrap();

	let listed_keys = kv_store.list(namespace).unwrap();
	assert_eq!(listed_keys.len(), 0);
}

// Integration-test the given KVStore implementation. Test relaying a few payments and check that
// the persisted data is updated the appropriate number of times.
pub(crate) fn do_test_store<K: KVStore>(store_0: &K, store_1: &K) {
	let chanmon_cfgs = create_chanmon_cfgs(2);
	let mut node_cfgs = create_node_cfgs(2, &chanmon_cfgs);
	let chain_mon_0 = test_utils::TestChainMonitor::new(Some(&chanmon_cfgs[0].chain_source), &chanmon_cfgs[0].tx_broadcaster, &chanmon_cfgs[0].logger, &chanmon_cfgs[0].fee_estimator, store_0, node_cfgs[0].keys_manager);
	let chain_mon_1 = test_utils::TestChainMonitor::new(Some(&chanmon_cfgs[1].chain_source), &chanmon_cfgs[1].tx_broadcaster, &chanmon_cfgs[1].logger, &chanmon_cfgs[1].fee_estimator, store_1, node_cfgs[1].keys_manager);
	node_cfgs[0].chain_monitor = chain_mon_0;
	node_cfgs[1].chain_monitor = chain_mon_1;
	let node_chanmgrs = create_node_chanmgrs(2, &node_cfgs, &[None, None]);
	let nodes = create_network(2, &node_cfgs, &node_chanmgrs);

	// Check that the persisted channel data is empty before any channels are
	// open.
	let mut persisted_chan_data_0 = read_channel_monitors(store_0, nodes[0].keys_manager, nodes[0].keys_manager).unwrap();
	assert_eq!(persisted_chan_data_0.len(), 0);
	let mut persisted_chan_data_1 = read_channel_monitors(store_1, nodes[1].keys_manager, nodes[1].keys_manager).unwrap();
	assert_eq!(persisted_chan_data_1.len(), 0);

	// Helper to make sure the channel is on the expected update ID.
	macro_rules! check_persisted_data {
		($expected_update_id: expr) => {
			persisted_chan_data_0 = read_channel_monitors(store_0, nodes[0].keys_manager, nodes[0].keys_manager).unwrap();
			assert_eq!(persisted_chan_data_0.len(), 1);
			for (_, mon) in persisted_chan_data_0.iter() {
				assert_eq!(mon.get_latest_update_id(), $expected_update_id);
			}
			persisted_chan_data_1 = read_channel_monitors(store_1, nodes[1].keys_manager, nodes[1].keys_manager).unwrap();
			assert_eq!(persisted_chan_data_1.len(), 1);
			for (_, mon) in persisted_chan_data_1.iter() {
				assert_eq!(mon.get_latest_update_id(), $expected_update_id);
			}
		}
	}

	// Create some initial channel and check that a channel was persisted.
	let _ = create_announced_chan_between_nodes(&nodes, 0, 1);
	check_persisted_data!(0);

	// Send a few payments and make sure the monitors are updated to the latest.
	send_payment(&nodes[0], &vec!(&nodes[1])[..], 8000000);
	check_persisted_data!(5);
	send_payment(&nodes[1], &vec!(&nodes[0])[..], 4000000);
	check_persisted_data!(10);

	// Force close because cooperative close doesn't result in any persisted
	// updates.
	nodes[0].node.force_close_broadcasting_latest_txn(&nodes[0].node.list_channels()[0].channel_id, &nodes[1].node.get_our_node_id()).unwrap();
	check_closed_event!(nodes[0], 1, ClosureReason::HolderForceClosed, [nodes[1].node.get_our_node_id()], 100000);
	check_closed_broadcast!(nodes[0], true);
	check_added_monitors!(nodes[0], 1);

	let node_txn = nodes[0].tx_broadcaster.txn_broadcasted.lock().unwrap();
	assert_eq!(node_txn.len(), 1);

	connect_block(&nodes[1], &create_dummy_block(nodes[0].best_block_hash(), 42, vec![node_txn[0].clone(), node_txn[0].clone()]));
	check_closed_broadcast!(nodes[1], true);
	check_closed_event!(nodes[1], 1, ClosureReason::CommitmentTxConfirmed, [nodes[0].node.get_our_node_id()], 100000);
	check_added_monitors!(nodes[1], 1);

	// Make sure everything is persisted as expected after close.
	check_persisted_data!(CLOSED_CHANNEL_UPDATE_ID);
}
