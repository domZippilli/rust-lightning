//! Provides utilities for LDK data persistence and retrieval.
//
// TODO: Prefix these with `rustdoc::` when we update our MSRV to be >= 1.52 to remove warnings.
#![deny(broken_intra_doc_links)]
#![deny(private_intra_doc_links)]

#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

pub mod fs_store;

#[cfg(test)]
mod test_utils;
