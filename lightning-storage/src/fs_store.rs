//! Objects related to [`FilesystemStore`] live here.
use lightning::util::persist::KVStore;

use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

#[cfg(not(target_os = "windows"))]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "windows")]
use {std::ffi::OsStr, std::os::windows::ffi::OsStrExt};

#[cfg(target_os = "windows")]
macro_rules! call {
	($e: expr) => {
		if $e != 0 {
			return Ok(());
		} else {
			return Err(std::io::Error::last_os_error());
		}
	};
}

#[cfg(target_os = "windows")]
fn path_to_windows_str<T: AsRef<OsStr>>(path: T) -> Vec<u16> {
	path.as_ref().encode_wide().chain(Some(0)).collect()
}

/// A [`KVStore`] implementation that writes to and reads from the file system.
pub struct FilesystemStore {
	data_dir: PathBuf,
	locks: Mutex<HashMap<(String, String), Arc<RwLock<()>>>>,
}

impl FilesystemStore {
	/// Constructs a new [`FilesystemStore`].
	pub fn new(data_dir: PathBuf) -> Self {
		let locks = Mutex::new(HashMap::new());
		Self { data_dir, locks }
	}

	/// Returns the data directory.
	pub fn get_data_dir(&self) -> PathBuf {
		self.data_dir.clone()
	}
}

impl KVStore for FilesystemStore {
	type Reader = FilesystemReader;

	fn read(&self, namespace: &str, key: &str) -> std::io::Result<Self::Reader> {
		let mut outer_lock = self.locks.lock().unwrap();
		let lock_key = (namespace.to_string(), key.to_string());
		let inner_lock_ref = Arc::clone(&outer_lock.entry(lock_key).or_default());

		if key.is_empty() {
			let msg = format!("Failed to read {}/{}: key may not be empty.", namespace, key);
			return Err(std::io::Error::new(std::io::ErrorKind::Other, msg));
		}

		let mut dest_file_path = self.data_dir.clone();
		dest_file_path.push(namespace);
		dest_file_path.push(key);
		FilesystemReader::new(dest_file_path, inner_lock_ref)
	}

	fn write(&self, namespace: &str, key: &str, buf: &[u8]) -> std::io::Result<()> {
		let mut outer_lock = self.locks.lock().unwrap();
		let lock_key = (namespace.to_string(), key.to_string());
		let inner_lock_ref = Arc::clone(&outer_lock.entry(lock_key).or_default());
		let _guard = inner_lock_ref.write().unwrap();

		if key.is_empty() {
			let msg = format!("Failed to write {}/{}: key may not be empty.", namespace, key);
			return Err(std::io::Error::new(std::io::ErrorKind::Other, msg));
		}

		let mut dest_file_path = self.data_dir.clone();
		dest_file_path.push(namespace);
		dest_file_path.push(key);

		let parent_directory = dest_file_path
			.parent()
			.ok_or_else(|| {
				let msg =
					format!("Could not retrieve parent directory of {}.", dest_file_path.display());
				std::io::Error::new(std::io::ErrorKind::InvalidInput, msg)
			})?
			.to_path_buf();
		fs::create_dir_all(&parent_directory)?;

		// Do a crazy dance with lots of fsync()s to be overly cautious here...
		// We never want to end up in a state where we've lost the old data, or end up using the
		// old data on power loss after we've returned.
		// The way to atomically write a file on Unix platforms is:
		// open(tmpname), write(tmpfile), fsync(tmpfile), close(tmpfile), rename(), fsync(dir)
		let mut tmp_file_path = dest_file_path.clone();
		tmp_file_path.set_extension("tmp");

		{
			let mut tmp_file = fs::File::create(&tmp_file_path)?;
			tmp_file.write_all(&buf)?;
			tmp_file.sync_all()?;
		}

		#[cfg(not(target_os = "windows"))]
		{
			fs::rename(&tmp_file_path, &dest_file_path)?;
			let dir_file = fs::OpenOptions::new().read(true).open(&parent_directory)?;
			unsafe {
				libc::fsync(dir_file.as_raw_fd());
			}
			Ok(())
		}

		#[cfg(target_os = "windows")]
		{
			if dest_file_path.exists() {
				call!(unsafe {
					windows_sys::Win32::Storage::FileSystem::ReplaceFileW(
						path_to_windows_str(dest_file_path).as_ptr(),
						path_to_windows_str(tmp_file_path).as_ptr(),
						std::ptr::null(),
						windows_sys::Win32::Storage::FileSystem::REPLACEFILE_IGNORE_MERGE_ERRORS,
						std::ptr::null_mut() as *const core::ffi::c_void,
						std::ptr::null_mut() as *const core::ffi::c_void,
					)
				});
			} else {
				call!(unsafe {
					windows_sys::Win32::Storage::FileSystem::MoveFileExW(
						path_to_windows_str(tmp_file_path).as_ptr(),
						path_to_windows_str(dest_file_path).as_ptr(),
						windows_sys::Win32::Storage::FileSystem::MOVEFILE_WRITE_THROUGH
							| windows_sys::Win32::Storage::FileSystem::MOVEFILE_REPLACE_EXISTING,
					)
				});
			}
		}
	}

	fn remove(&self, namespace: &str, key: &str) -> std::io::Result<()> {
		let mut outer_lock = self.locks.lock().unwrap();
		let lock_key = (namespace.to_string(), key.to_string());
		let inner_lock_ref = Arc::clone(&outer_lock.entry(lock_key.clone()).or_default());

		let _guard = inner_lock_ref.write().unwrap();

		if key.is_empty() {
			let msg = format!("Failed to remove {}/{}: key may not be empty.", namespace, key);
			return Err(std::io::Error::new(std::io::ErrorKind::Other, msg));
		}

		let mut dest_file_path = self.data_dir.clone();
		dest_file_path.push(namespace);
		dest_file_path.push(key);

		if !dest_file_path.is_file() {
			return Ok(());
		}

		fs::remove_file(&dest_file_path)?;
		#[cfg(not(target_os = "windows"))]
		{
			let parent_directory = dest_file_path.parent().ok_or_else(|| {
				let msg =
					format!("Could not retrieve parent directory of {}.", dest_file_path.display());
				std::io::Error::new(std::io::ErrorKind::InvalidInput, msg)
			})?;
			let dir_file = fs::OpenOptions::new().read(true).open(parent_directory)?;
			unsafe {
				// The above call to `fs::remove_file` corresponds to POSIX `unlink`, whose changes
				// to the inode might get cached (and hence possibly lost on crash), depending on
				// the target platform and file system.
				//
				// In order to assert we permanently removed the file in question we therefore
				// call `fsync` on the parent directory on platforms that support it,
				libc::fsync(dir_file.as_raw_fd());
			}
		}

		if dest_file_path.is_file() {
			return Err(std::io::Error::new(std::io::ErrorKind::Other, "Removing key failed"));
		}

		if Arc::strong_count(&inner_lock_ref) == 2 {
			// It's safe to remove the lock entry if we're the only one left holding a strong
			// reference. Checking this is necessary to ensure we continue to distribute references to the
			// same lock as long as some Readers are around. However, we still want to
			// clean up the table when possible.
			//
			// Note that this by itself is still leaky as lock entries will remain when more Readers/Writers are
			// around, but is preferable to doing nothing *or* something overly complex such as
			// implementing yet another RAII structure just for this pupose.
			outer_lock.remove(&lock_key);
		}

		// Garbage collect all lock entries that are not referenced anymore.
		outer_lock.retain(|_, v| Arc::strong_count(&v) > 1);

		Ok(())
	}

	fn list(&self, namespace: &str) -> std::io::Result<Vec<String>> {
		let mut prefixed_dest = self.data_dir.clone();
		prefixed_dest.push(namespace);

		let mut keys = Vec::new();

		if !Path::new(&prefixed_dest).exists() {
			return Ok(Vec::new());
		}

		for entry in fs::read_dir(&prefixed_dest)? {
			let entry = entry?;
			let p = entry.path();

			if !p.is_file() {
				continue;
			}

			if let Some(ext) = p.extension() {
				if ext == "tmp" {
					continue;
				}
			}

			if let Ok(relative_path) = p.strip_prefix(&prefixed_dest) {
				keys.push(relative_path.display().to_string())
			}
		}

		Ok(keys)
	}
}

/// A buffered [`Read`] implementation as returned from [`FilesystemStore::read`].
pub struct FilesystemReader {
	inner: BufReader<fs::File>,
	lock_ref: Arc<RwLock<()>>,
}

impl FilesystemReader {
	fn new(dest_file_path: PathBuf, lock_ref: Arc<RwLock<()>>) -> std::io::Result<Self> {
		let f = fs::File::open(dest_file_path.clone())?;
		let inner = BufReader::new(f);
		Ok(Self { inner, lock_ref })
	}
}

impl Read for FilesystemReader {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		let _guard = self.lock_ref.read().unwrap();
		self.inner.read(buf)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::test_utils::{do_read_write_remove_list_persist, do_test_store};

	use bitcoin::hashes::hex::FromHex;
	use bitcoin::Txid;

	use lightning::chain::ChannelMonitorUpdateStatus;
	use lightning::chain::chainmonitor::Persist;
	use lightning::chain::transaction::OutPoint;
	use lightning::check_closed_event;
	use lightning::events::{ClosureReason, MessageSendEventsProvider};
	use lightning::ln::functional_test_utils::*;
	use lightning::util::test_utils;
	use lightning::util::persist::read_channel_monitors;
	use std::fs;
	#[cfg(target_os = "windows")]
	use {
		lightning::get_event_msg,
		lightning::ln::msgs::ChannelMessageHandler,
	};

	impl Drop for FilesystemStore{
		fn drop(&mut self) {
			// We test for invalid directory names, so it's OK if directory removal
			// fails.
			match fs::remove_dir_all(&self.data_dir) {
				Err(e) => println!("Failed to remove test persister directory: {}", e),
				_ => {}
			}
		}
	}

	#[test]
	fn read_write_remove_list_persist() {
		let temp_path = std::env::temp_dir();
		let fs_store = FilesystemStore::new(temp_path);
		do_read_write_remove_list_persist(&fs_store);
	}

	#[test]
	fn test_if_monitors_is_not_dir() {
		let store = FilesystemStore::new("test_monitors_is_not_dir".into());

		fs::create_dir_all(&store.get_data_dir()).unwrap();
		let mut path = std::path::PathBuf::from(&store.get_data_dir());
		path.push("monitors");
		fs::File::create(path).unwrap();

		let chanmon_cfgs = create_chanmon_cfgs(1);
		let mut node_cfgs = create_node_cfgs(1, &chanmon_cfgs);
		let chain_mon_0 = test_utils::TestChainMonitor::new(Some(&chanmon_cfgs[0].chain_source), &chanmon_cfgs[0].tx_broadcaster, &chanmon_cfgs[0].logger, &chanmon_cfgs[0].fee_estimator, &store, node_cfgs[0].keys_manager);
		node_cfgs[0].chain_monitor = chain_mon_0;
		let node_chanmgrs = create_node_chanmgrs(1, &node_cfgs, &[None]);
		let nodes = create_network(1, &node_cfgs, &node_chanmgrs);

		// Check that read_channel_monitors() returns error if monitors/ is not a
		// directory.
		assert!(read_channel_monitors(&store, nodes[0].keys_manager, nodes[0].keys_manager).is_err());
	}

	#[test]
	fn test_filesystem_store() {
		// Create the nodes, giving them FilesystemStores for data stores.
		let store_0 = FilesystemStore::new("test_filesystem_store_0".into());
		let store_1 = FilesystemStore::new("test_filesystem_store_1".into());
		do_test_store(&store_0, &store_1)
	}

	// Test that if the store's path to channel data is read-only, writing a
	// monitor to it results in the store returning a PermanentFailure.
	// Windows ignores the read-only flag for folders, so this test is Unix-only.
	#[cfg(not(target_os = "windows"))]
	#[test]
	fn test_readonly_dir_perm_failure() {
		let store = FilesystemStore::new("test_readonly_dir_perm_failure".into());
		fs::create_dir_all(&store.get_data_dir()).unwrap();

		// Set up a dummy channel and force close. This will produce a monitor
		// that we can then use to test persistence.
		let chanmon_cfgs = create_chanmon_cfgs(2);
		let node_cfgs = create_node_cfgs(2, &chanmon_cfgs);
		let node_chanmgrs = create_node_chanmgrs(2, &node_cfgs, &[None, None]);
		let nodes = create_network(2, &node_cfgs, &node_chanmgrs);
		let chan = create_announced_chan_between_nodes(&nodes, 0, 1);
		nodes[1].node.force_close_broadcasting_latest_txn(&chan.2, &nodes[0].node.get_our_node_id()).unwrap();
		check_closed_event!(nodes[1], 1, ClosureReason::HolderForceClosed, [nodes[0].node.get_our_node_id()], 100000);
		let mut added_monitors = nodes[1].chain_monitor.added_monitors.lock().unwrap();
		let update_map = nodes[1].chain_monitor.latest_monitor_update_id.lock().unwrap();
		let update_id = update_map.get(&added_monitors[0].0.to_channel_id()).unwrap();

		// Set the store's directory to read-only, which should result in
		// returning a permanent failure when we then attempt to persist a
		// channel update.
		let path = &store.get_data_dir();
		let mut perms = fs::metadata(path).unwrap().permissions();
		perms.set_readonly(true);
		fs::set_permissions(path, perms).unwrap();

		let test_txo = OutPoint {
			txid: Txid::from_hex("8984484a580b825b9972d7adb15050b3ab624ccd731946b3eeddb92f4e7ef6be").unwrap(),
			index: 0
		};
		match store.persist_new_channel(test_txo, &added_monitors[0].1, update_id.2) {
			ChannelMonitorUpdateStatus::PermanentFailure => {},
			_ => panic!("unexpected result from persisting new channel")
		}

		nodes[1].node.get_and_clear_pending_msg_events();
		added_monitors.clear();
	}

	// Test that if a store's directory name is invalid, monitor persistence
	// will fail.
	#[cfg(target_os = "windows")]
	#[test]
	fn test_fail_on_open() {
		// Set up a dummy channel and force close. This will produce a monitor
		// that we can then use to test persistence.
		let chanmon_cfgs = create_chanmon_cfgs(2);
		let mut node_cfgs = create_node_cfgs(2, &chanmon_cfgs);
		let node_chanmgrs = create_node_chanmgrs(2, &node_cfgs, &[None, None]);
		let nodes = create_network(2, &node_cfgs, &node_chanmgrs);
		let chan = create_announced_chan_between_nodes(&nodes, 0, 1);
		nodes[1].node.force_close_broadcasting_latest_txn(&chan.2, &nodes[0].node.get_our_node_id()).unwrap();
		check_closed_event!(nodes[1], 1, ClosureReason::HolderForceClosed, [nodes[0].node.get_our_node_id()], 100000);
		let mut added_monitors = nodes[1].chain_monitor.added_monitors.lock().unwrap();
		let update_map = nodes[1].chain_monitor.latest_monitor_update_id.lock().unwrap();
		let update_id = update_map.get(&added_monitors[0].0.to_channel_id()).unwrap();

		// Create the store with an invalid directory name and test that the
		// channel fails to open because the directories fail to be created. There
		// don't seem to be invalid filename characters on Unix that Rust doesn't
		// handle, hence why the test is Windows-only.
		let store = FilesystemStore::new(":<>/".into());

		let test_txo = OutPoint {
			txid: Txid::from_hex("8984484a580b825b9972d7adb15050b3ab624ccd731946b3eeddb92f4e7ef6be").unwrap(),
			index: 0
		};
		match store.persist_new_channel(test_txo, &added_monitors[0].1, update_id.2) {
			ChannelMonitorUpdateStatus::PermanentFailure => {},
			_ => panic!("unexpected result from persisting new channel")
		}

		nodes[1].node.get_and_clear_pending_msg_events();
		added_monitors.clear();
	}
}
