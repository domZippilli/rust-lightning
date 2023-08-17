// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE
// or http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your option.
// You may not use this file except in accordance with one or both of these
// licenses.

//! This module contains a simple key-value store trait KVStorePersister that
//! allows one to implement the persistence for [`ChannelManager`], [`NetworkGraph`],
//! and [`ChannelMonitor`] all in one place.

use core::convert::{TryFrom, TryInto};
use core::ops::Deref;
use bitcoin::hashes::hex::{FromHex, ToHex};
use bitcoin::{BlockHash, Txid};

use crate::io;
use crate::ln::msgs::DecodeError;
use crate::prelude::{Vec, String};
use crate::routing::scoring::WriteableScore;

use crate::chain;
use crate::chain::chaininterface::{BroadcasterInterface, FeeEstimator};
use crate::chain::chainmonitor::{Persist, MonitorUpdateId};
use crate::sign::{EntropySource, NodeSigner, WriteableEcdsaChannelSigner, SignerProvider};
use crate::chain::transaction::OutPoint;
use crate::chain::channelmonitor::{ChannelMonitor, ChannelMonitorUpdate, CLOSED_CHANNEL_UPDATE_ID};
use crate::ln::channelmanager::ChannelManager;
use crate::routing::router::Router;
use crate::routing::gossip::NetworkGraph;
use crate::util::logger::Logger;
use crate::util::ser::{Readable, ReadableArgs, Writeable};

/// The namespace under which the [`ChannelManager`] will be persisted.
pub const CHANNEL_MANAGER_PERSISTENCE_NAMESPACE: &str = "";
/// The key under which the [`ChannelManager`] will be persisted.
pub const CHANNEL_MANAGER_PERSISTENCE_KEY: &str = "manager";

/// The namespace under which [`ChannelMonitor`]s will be persisted.
pub const CHANNEL_MONITOR_PERSISTENCE_NAMESPACE: &str = "monitors";
/// The namespace under which [`ChannelMonitorUpdate`]s will be persisted.
pub const CHANNEL_MONITOR_UPDATE_PERSISTENCE_NAMESPACE: &str = "monitors/updates";

/// The namespace under which the [`NetworkGraph`] will be persisted.
pub const NETWORK_GRAPH_PERSISTENCE_NAMESPACE: &str = "";
/// The key under which the [`NetworkGraph`] will be persisted.
pub const NETWORK_GRAPH_PERSISTENCE_KEY: &str = "network_graph";

/// The namespace under which the [`WriteableScore`] will be persisted.
pub const SCORER_PERSISTENCE_NAMESPACE: &str = "";
/// The key under which the [`WriteableScore`] will be persisted.
pub const SCORER_PERSISTENCE_KEY: &str = "scorer";

/// Provides an interface that allows to store and retrieve persisted values that are associated
/// with given keys.
///
/// In order to avoid collisions the key space is segmented based on the given `namespace`s.
/// Implementations of this trait are free to handle them in different ways, as long as
/// per-namespace key uniqueness is asserted.
///
/// Keys and namespaces are required to be valid ASCII strings and the empty namespace (`""`) is
/// assumed to be valid namespace.
pub trait KVStore {
	/// A reader as returned by [`Self::read`].
	type Reader: io::Read;
	/// Returns an [`io::Read`] for the given `namespace` and `key` from which [`Readable`]s may be
	/// read.
	///
	/// Returns an [`ErrorKind::NotFound`] if the given `key` could not be found in the given `namespace`.
	///
	/// [`Readable`]: crate::util::ser::Readable
	/// [`ErrorKind::NotFound`]: io::ErrorKind::NotFound
	fn read(&self, namespace: &str, key: &str) -> io::Result<Self::Reader>;
	/// Persists the given data under the given `key`.
	///
	/// Will create the given `namespace` if not already present in the store.
	fn write(&self, namespace: &str, key: &str, buf: &[u8]) -> io::Result<()>;
	/// Removes any data that had previously been persisted under the given `key`.
	fn remove(&self, namespace: &str, key: &str) -> io::Result<()>;
	/// Returns a list of keys that are stored under the given `namespace`.
	///
	/// Will return an empty list if the `namespace` is unknown.
	fn list(&self, namespace: &str) -> io::Result<Vec<String>>;
}

/// Trait that handles persisting a [`ChannelManager`], [`NetworkGraph`], and [`WriteableScore`] to disk.
pub trait Persister<'a, M: Deref, T: Deref, ES: Deref, NS: Deref, SP: Deref, F: Deref, R: Deref, L: Deref, S: WriteableScore<'a>>
	where M::Target: 'static + chain::Watch<<SP::Target as SignerProvider>::Signer>,
		T::Target: 'static + BroadcasterInterface,
		ES::Target: 'static + EntropySource,
		NS::Target: 'static + NodeSigner,
		SP::Target: 'static + SignerProvider,
		F::Target: 'static + FeeEstimator,
		R::Target: 'static + Router,
		L::Target: 'static + Logger,
{
	/// Persist the given ['ChannelManager'] to disk, returning an error if persistence failed.
	fn persist_manager(&self, channel_manager: &ChannelManager<M, T, ES, NS, SP, F, R, L>) -> Result<(), io::Error>;

	/// Persist the given [`NetworkGraph`] to disk, returning an error if persistence failed.
	fn persist_graph(&self, network_graph: &NetworkGraph<L>) -> Result<(), io::Error>;

	/// Persist the given [`WriteableScore`] to disk, returning an error if persistence failed.
	fn persist_scorer(&self, scorer: &S) -> Result<(), io::Error>;
}


impl<'a, A: KVStore, M: Deref, T: Deref, ES: Deref, NS: Deref, SP: Deref, F: Deref, R: Deref, L: Deref, S: WriteableScore<'a>> Persister<'a, M, T, ES, NS, SP, F, R, L, S> for A
	where M::Target: 'static + chain::Watch<<SP::Target as SignerProvider>::Signer>,
		T::Target: 'static + BroadcasterInterface,
		ES::Target: 'static + EntropySource,
		NS::Target: 'static + NodeSigner,
		SP::Target: 'static + SignerProvider,
		F::Target: 'static + FeeEstimator,
		R::Target: 'static + Router,
		L::Target: 'static + Logger,
{
	/// Persist the given ['ChannelManager'] to disk, returning an error if persistence failed.
	fn persist_manager(&self, channel_manager: &ChannelManager<M, T, ES, NS, SP, F, R, L>) -> Result<(), io::Error> {
		self.write(CHANNEL_MANAGER_PERSISTENCE_NAMESPACE, CHANNEL_MANAGER_PERSISTENCE_KEY, &channel_manager.encode())
	}

	/// Persist the given [`NetworkGraph`] to disk, returning an error if persistence failed.
	fn persist_graph(&self, network_graph: &NetworkGraph<L>) -> Result<(), io::Error> {
		self.write(NETWORK_GRAPH_PERSISTENCE_NAMESPACE, NETWORK_GRAPH_PERSISTENCE_KEY, &network_graph.encode())
	}

	/// Persist the given [`WriteableScore`] to disk, returning an error if persistence failed.
	fn persist_scorer(&self, scorer: &S) -> Result<(), io::Error> {
		self.write(SCORER_PERSISTENCE_NAMESPACE, SCORER_PERSISTENCE_KEY, &scorer.encode())
	}
}

// impl<ChannelSigner: WriteableEcdsaChannelSigner, K: KVStore> Persist<ChannelSigner> for K {
// 	// TODO: We really need a way for the persister to inform the user that its time to crash/shut
// 	// down once these start returning failure.
// 	// A PermanentFailure implies we should probably just shut down the node since we're
// 	// force-closing channels without even broadcasting!

// 	fn persist_new_channel(&self, funding_txo: OutPoint, monitor: &ChannelMonitor<ChannelSigner>, _update_id: MonitorUpdateId) -> chain::ChannelMonitorUpdateStatus {
// 		let key = format!("{}_{}", funding_txo.txid.to_hex(), funding_txo.index);
// 		match self.write(CHANNEL_MONITOR_PERSISTENCE_NAMESPACE, &key, &monitor.encode()) {
// 			Ok(()) => chain::ChannelMonitorUpdateStatus::Completed,
// 			Err(_) => chain::ChannelMonitorUpdateStatus::PermanentFailure,
// 		}
// 	}

// 	fn update_persisted_channel(&self, funding_txo: OutPoint, _update: Option<&ChannelMonitorUpdate>, monitor: &ChannelMonitor<ChannelSigner>, _update_id: MonitorUpdateId) -> chain::ChannelMonitorUpdateStatus {
// 		let key = format!("{}_{}", funding_txo.txid.to_hex(), funding_txo.index);
// 		match self.write(CHANNEL_MONITOR_PERSISTENCE_NAMESPACE, &key, &monitor.encode()) {
// 			Ok(()) => chain::ChannelMonitorUpdateStatus::Completed,
// 			Err(_) => chain::ChannelMonitorUpdateStatus::PermanentFailure,
// 		}
// 	}
// }

/// Read previously persisted [`ChannelMonitor`]s from the store.
pub fn read_channel_monitors<K: Deref, ES: Deref, SP: Deref>(
	kv_store: K, entropy_source: ES, signer_provider: SP,
) -> crate::io::Result<Vec<(BlockHash, ChannelMonitor<<SP::Target as SignerProvider>::Signer>)>>
where
	K::Target: KVStore,
	ES::Target: EntropySource + Sized,
	SP::Target: SignerProvider + Sized,
{
	let mut res = Vec::new();

	for stored_key in kv_store.list(CHANNEL_MONITOR_PERSISTENCE_NAMESPACE)? {
		let txid = Txid::from_hex(stored_key.split_at(64).0).map_err(|_| {
			crate::io::Error::new(crate::io::ErrorKind::InvalidData, "Invalid tx ID in stored key")
		})?;

		let index: u16 = stored_key.split_at(65).1.parse().map_err(|_| {
			crate::io::Error::new(crate::io::ErrorKind::InvalidData, "Invalid tx index in stored key")
		})?;

		match <(BlockHash, ChannelMonitor<<SP::Target as SignerProvider>::Signer>)>::read(
			&mut kv_store.read(CHANNEL_MONITOR_PERSISTENCE_NAMESPACE, &stored_key)?,
			(&*entropy_source, &*signer_provider),
		) {
			Ok((block_hash, channel_monitor)) => {
				if channel_monitor.get_funding_txo().0.txid != txid
					|| channel_monitor.get_funding_txo().0.index != index
				{
					return Err(crate::io::Error::new(
						crate::io::ErrorKind::InvalidData,
						"ChannelMonitor was stored under the wrong key",
					));
				}
				res.push((block_hash, channel_monitor));
			}
			Err(_) => {
				return Err(crate::io::Error::new(
					crate::io::ErrorKind::InvalidData,
					"Failed to deserialize ChannelMonitor"
				))
			}
		}
	}
	Ok(res)
}

enum KVStoreChannelMonitorReaderError {
	/// The monitor name was improperly formatted.
	BadMonitorName(String, String),
	/// The monitor could not be decoded.
	MonitorDecodeFailed(DecodeError, String),
	/// The update could not be decoded.
	UpdateDecodeFailed(DecodeError, String),
	/// Storage could not be read.
	StorageReadFailed(io::Error, String),
	/// An update could not be applied to a monitor.
	UpdateFailed(String, String),
}

impl From<KVStoreChannelMonitorReaderError> for io::Error {
	fn from(value: KVStoreChannelMonitorReaderError) -> Self {
		match value {
			KVStoreChannelMonitorReaderError::BadMonitorName(reason, context) => {
				io::Error::new(io::ErrorKind::InvalidInput, format!("{reason}, context: {context}'"))
			},
			KVStoreChannelMonitorReaderError::MonitorDecodeFailed(reason, context) => {
				io::Error::new(io::ErrorKind::InvalidData, format!("{reason}, context: {context:?}'"))
			},
    		KVStoreChannelMonitorReaderError::UpdateDecodeFailed(reason, context) => {
				io::Error::new(io::ErrorKind::InvalidData, format!("{reason}, context: {context:?}'"))
			},
			KVStoreChannelMonitorReaderError::StorageReadFailed(reason, context) => {
				io::Error::new(io::ErrorKind::InvalidData, format!("{reason}, context: {context:?}'"))
			},
    		KVStoreChannelMonitorReaderError::UpdateFailed(reason, context) => {
				io::Error::new(io::ErrorKind::InvalidData, format!("{reason}, context: {context}'"))
			},
		}
	}
}

/// A struct representing a name for a monitor.
#[derive(Clone, Debug)]
pub struct MonitorName(String);

impl TryFrom<MonitorName> for OutPoint {
	type Error = std::io::Error;

	fn try_from(value: MonitorName) -> Result<Self, io::Error> {
		let (txid_hex, index) = value.0.split_once('_').ok_or_else(|| {
			KVStoreChannelMonitorReaderError::BadMonitorName("no underscore".to_string(), value.0.clone())
		})?;
		let index = index.parse().map_err(|e| {
			KVStoreChannelMonitorReaderError::BadMonitorName(
				format!("bad index value, caused by {e}"),
				value.0.clone(),
			)
		})?;
		let txid = Txid::from_hex(txid_hex).map_err(|e| {
			KVStoreChannelMonitorReaderError::BadMonitorName(
				format!("bad txid, caused by: {e}"),
				value.0.clone(),
			)
		})?;
		Ok(OutPoint { txid, index })
	}
}

impl From<OutPoint> for MonitorName {
	fn from(value: OutPoint) -> Self {
		MonitorName(format!("{}_{}", value.txid.to_hex(), value.index))
	}
}

/// A struct representing a name for an update.
#[derive(Clone, Debug)]
pub struct UpdateName(String);

impl From<u64> for UpdateName {
	fn from(value: u64) -> Self {
		Self(format!("{:0>20}", value))
	}
}

#[allow(clippy::type_complexity)]
pub trait KVStoreChannelMonitorReader<K: KVStore> {
	fn read_channelmonitors<ES: Deref + Clone, SP: Deref + Clone, B: Deref, F: Deref + Clone, L: Deref>(
		&self, entropy_source: ES, signer_provider: SP, broadcaster: &B, fee_estimator: F,
		logger: &L,
	) -> std::io::Result<Vec<(BlockHash, ChannelMonitor<<SP::Target as SignerProvider>::Signer>)>>
	where
		ES::Target: EntropySource + Sized,
		SP::Target: SignerProvider + Sized,
		B::Target: BroadcasterInterface,
		F::Target: FeeEstimator,
		L::Target: Logger;
	/// List all the names of monitors.
	fn list_monitor_names(&self) -> io::Result<Vec<MonitorName>>;
	/// Key to a specific monitor.
	fn monitor_key(&self, monitor_name: &MonitorName) -> String;
	/// Deserialize a channel monitor.
	fn deserialize_monitor<ES: Deref, SP: Deref>(
		&self, entropy_source: ES, signer_provider: SP, monitor_name: MonitorName,
	) -> io::Result<(BlockHash, ChannelMonitor<<SP::Target as SignerProvider>::Signer>)>
	where
		ES::Target: EntropySource + Sized,
		SP::Target: SignerProvider + Sized;
	/// List all the names of updates corresponding to a given monitor name.
	fn list_update_names(&self, monitor_name: &MonitorName) -> io::Result<Vec<UpdateName>>;
	/// Path to corresponding update directory for a given monitor name.
	fn path_to_monitor_updates(&self, monitor_name: &MonitorName) -> String;
	/// Deserialize a channel monitor update.
	fn deserialize_monitor_update(
		&self, monitor_name: &MonitorName, update_name: &UpdateName,
	) -> io::Result<ChannelMonitorUpdate>;
	/// Key to a specific update.
	fn update_key(&self, monitor_name: &MonitorName, update_name: &UpdateName) -> String;
	/// Delete updates with an update_id lower than the given channel monitor.
	fn delete_stale_updates<ChannelSigner: WriteableEcdsaChannelSigner>(
		&self, channel_id: OutPoint, monitor: &ChannelMonitor<ChannelSigner>,
	) -> io::Result<()>;
}

impl<K: KVStore> KVStoreChannelMonitorReader<K> for K {
	fn read_channelmonitors<
		ES: Deref + Clone,
		SP: Deref + Clone,
		B: Deref,
		F: Deref + Clone,
		L: Deref,
	>(
		&self, entropy_source: ES, signer_provider: SP, broadcaster: &B, fee_estimator: F,
		logger: &L,
	) -> std::io::Result<Vec<(BlockHash, ChannelMonitor<<SP::Target as SignerProvider>::Signer>)>>
	where
		ES::Target: EntropySource + Sized,
		SP::Target: SignerProvider + Sized,
		B::Target: BroadcasterInterface,
		F::Target: FeeEstimator,
		L::Target: Logger
	{
		let mut res = Vec::new();
		// for each monitor...
		for monitor_name in self.list_monitor_names()? {
			// ...parse the monitor
			let (bh, monitor) = self.deserialize_monitor(
				entropy_source.clone(),
				signer_provider.clone(),
				monitor_name.clone(),
			)?;
			// ...parse and apply the updates with an id higher than the monitor.
			for update_name in self.list_update_names(&monitor_name)? {
				let update = self.deserialize_monitor_update(&monitor_name, &update_name)?;
				if update.update_id == CLOSED_CHANNEL_UPDATE_ID
					|| update.update_id > monitor.get_latest_update_id()
				{
					monitor
						.update_monitor(&update, broadcaster, fee_estimator.clone(), logger)
						.map_err(|_| {
							KVStoreChannelMonitorReaderError::UpdateFailed(
								"update_monitor returned Err(())".to_string(),
								format!("monitor: {:?}", monitor_name),
							)
						})?;
				}
			}
			// ...push the result into the return vec
			res.push((bh, monitor))
		}
		Ok(res)
	}

	/// Key to a specific monitor.
	fn monitor_key(&self, monitor_name: &MonitorName) -> String {
		CHANNEL_MONITOR_PERSISTENCE_NAMESPACE.to_owned() + &monitor_name.0
	}

	/// Key to a specific update.
	fn update_key(&self, monitor_name: &MonitorName, update_name: &UpdateName) -> String {
		self.path_to_monitor_updates(monitor_name) + &update_name.0
	}

	/// List all the names of monitors.
	fn list_monitor_names(&self) -> io::Result<Vec<MonitorName>> {
		Ok(self.list(CHANNEL_MONITOR_PERSISTENCE_NAMESPACE)?.into_iter().map(MonitorName).collect())
	}

	/// List all the names of updates corresponding to a given monitor name.
	fn list_update_names(&self, monitor_name: &MonitorName) -> io::Result<Vec<UpdateName>> {
		let update_dir_path = self.path_to_monitor_updates(monitor_name);
		Ok(self.list(&update_dir_path)?.into_iter().map(UpdateName).collect())
	}

	/// Path to corresponding update directory for a given monitor name.
	fn path_to_monitor_updates(&self, monitor_name: &MonitorName) -> String {
		CHANNEL_MONITOR_UPDATE_PERSISTENCE_NAMESPACE.to_owned() + &monitor_name.0
	}

	/// Deserialize a channel monitor.
	fn deserialize_monitor<ES: Deref, SP: Deref>(
		&self, entropy_source: ES, signer_provider: SP, monitor_name: MonitorName,
	) -> io::Result<(BlockHash, ChannelMonitor<<SP::Target as SignerProvider>::Signer>)>
	where
		ES::Target: EntropySource + Sized,
		SP::Target: SignerProvider + Sized
	{
		let key = self.monitor_key(&monitor_name);
		let outpoint: OutPoint = monitor_name.try_into()?;
		match <(BlockHash, ChannelMonitor<<SP::Target as SignerProvider>::Signer>)>::read(
			&mut self.read(CHANNEL_MONITOR_PERSISTENCE_NAMESPACE, &key)
					 .map_err(|e| KVStoreChannelMonitorReaderError::StorageReadFailed(e, key.to_owned()))?,
			(&*entropy_source, &*signer_provider),
		) {
			Ok((blockhash, channel_monitor)) => {
				if channel_monitor.get_funding_txo().0.txid != outpoint.txid
					|| channel_monitor.get_funding_txo().0.index != outpoint.index
				{
					return Err(KVStoreChannelMonitorReaderError::MonitorDecodeFailed(
						DecodeError::InvalidValue,
						key,
					)
					.into());
				}
				Ok((blockhash, channel_monitor))
			}
			Err(e) => Err(KVStoreChannelMonitorReaderError::MonitorDecodeFailed(e, key).into()),
		}
	}

	/// Deserialize a channel monitor update.
	fn deserialize_monitor_update(
		&self, monitor_name: &MonitorName, update_name: &UpdateName,
	) -> io::Result<ChannelMonitorUpdate>
	{
		let key = self.update_key(monitor_name, update_name);
		Ok(ChannelMonitorUpdate::read(&mut self
			.read(CHANNEL_MONITOR_UPDATE_PERSISTENCE_NAMESPACE, &key)
			.map_err(|e| KVStoreChannelMonitorReaderError::StorageReadFailed(e, key.to_owned()))?)
			.map_err(|e| KVStoreChannelMonitorReaderError::UpdateDecodeFailed(e, key))?)
	}

	/// Delete updates with an update_id lower than the given channel monitor.
	fn delete_stale_updates<ChannelSigner: WriteableEcdsaChannelSigner>(
		&self, channel_id: OutPoint, monitor: &ChannelMonitor<ChannelSigner>,
	) -> io::Result<()> 
	{
		let monitor_name: MonitorName = channel_id.into();
		let update_names =
			self.list_update_names(&monitor_name)?;
		for update_name in update_names {
			let update =
				self.deserialize_monitor_update(&monitor_name, &update_name)?;
			if update.update_id != CLOSED_CHANNEL_UPDATE_ID
				&& update.update_id <= monitor.get_latest_update_id()
			{
				self.remove(CHANNEL_MONITOR_UPDATE_PERSISTENCE_NAMESPACE, &self.update_key(&monitor_name, &update_name))?;
			}
		}
		Ok(())
	}
}

impl<ChannelSigner: WriteableEcdsaChannelSigner, K: KVStore + KVStoreChannelMonitorReader<K>> Persist<ChannelSigner> for K {
	// TODO: We really need a way for the persister to inform the user that its time to crash/shut
	// down once these start returning failure.
	// A PermanentFailure implies we should probably just shut down the node since we're
	// force-closing channels without even broadcasting!
	fn persist_new_channel(&self, funding_txo: OutPoint, monitor: &ChannelMonitor<ChannelSigner>, _update_id: MonitorUpdateId) -> chain::ChannelMonitorUpdateStatus 
	{
		let key = self.monitor_key(&funding_txo.into());
		match self.write(CHANNEL_MONITOR_PERSISTENCE_NAMESPACE, &key, &monitor.encode()) {
			Ok(()) => {
				if let Err(_e) = self.delete_stale_updates(funding_txo, monitor) {
					// TODO(domz): what to do? seems like an error or panic is needed, but OTOH cleanup is technically optional
					//log_error!(self.logger, "error cleaning up channel monitor updates! {}", e);
				};
				chain::ChannelMonitorUpdateStatus::Completed
			},
			Err(_) => chain::ChannelMonitorUpdateStatus::PermanentFailure,
		}
	}

	fn update_persisted_channel(
		&self, funding_txo: OutPoint, update: Option<&ChannelMonitorUpdate>,
		monitor: &ChannelMonitor<ChannelSigner>, update_id: MonitorUpdateId,
	) -> chain::ChannelMonitorUpdateStatus {
		match update {
			Some(update) => {
				// This is an update to the monitor, which we persist to apply on restart.
				// IMPORTANT: update_id: MonitorUpdateId is not to be confused with ChannelMonitorUpdate.update_id.
				// The first is an opaque identifier for this call (used for calling back write completion). The second
				// is the channel update sequence number.
				let key = self.update_key(&funding_txo.into(), &update.update_id.into());
				match self.write(CHANNEL_MONITOR_UPDATE_PERSISTENCE_NAMESPACE, &key, &update.encode()) {
					Ok(()) => chain::ChannelMonitorUpdateStatus::Completed,
					Err(_) => chain::ChannelMonitorUpdateStatus::PermanentFailure,
				}
			}
			// A new block. Now we need to persist the entire new monitor and discard the old
			// updates.
			None => self.persist_new_channel(funding_txo, monitor, update_id),
		}
	}
	
}