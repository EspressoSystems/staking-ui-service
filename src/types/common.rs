//! Primitive types.

use serde::{Deserialize, Serialize};
use surf_disco::Url;

pub use alloy::primitives::{Address, BlockHash, U256};
pub use tagged_base64::TaggedBase64;

/// An amount of Espresso tokens in WEI.
pub type ESPTokenAmount = U256;

/// A Unix timestamps in seconds since epoch.
pub type Timestamp = u64;

/// A ratio between 0 and 1.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct Ratio(f32);

impl Ratio {
    pub fn new(num: usize, den: usize) -> Self {
        Self((num as f32) / (den as f32))
    }
}

impl From<f32> for Ratio {
    fn from(value: f32) -> Self {
        Self(value)
    }
}

impl From<Ratio> for f32 {
    fn from(val: Ratio) -> Self {
        val.0
    }
}

/// An entry in the full node set.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct NodeSetEntry {
    /// Node's Ethereum address.
    pub address: Address,

    /// The key used for the node for signing consensus messages.
    pub staking_key: TaggedBase64,

    /// state verifying key
    pub state_key: TaggedBase64,

    /// Total stake currently attributed to the node.
    pub stake: ESPTokenAmount,

    /// How much commission the node charges.
    pub commission: Ratio,

    /// Optional metadata like a human-readable name and icon.
    ///
    /// May be [`None`] if no metadata URI is registered for this node.
    pub metadata: Option<NodeMetadata>,
}

/// Information about an L1 block.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct L1BlockInfo {
    /// The block number
    pub number: u64,

    /// The hash of this block (useful for detecting reorgs)
    pub hash: BlockHash,

    /// The timestamp of this block.
    pub timestamp: Timestamp,
}

/// Minimal information needed to identify an L1 block and check for reorgs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct L1BlockId {
    /// The block number.
    pub number: u64,

    /// The hash of this block.
    pub hash: BlockHash,

    /// The parent of this block, used for reorg detection.
    pub parent: BlockHash,
}

/// Information about the exiting of a node from the node set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct NodeExit {
    /// The exiting node.
    pub address: Address,

    /// The timestamp for the exit escrow delay time.
    pub exit_time: Timestamp,
}

/// Information about the current "time" on the Espresso chain.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct EpochAndBlock {
    /// The current epoch of the Espresso chain
    pub epoch: u64,

    /// The current block of the Espresso chain
    pub block: u64,

    /// The timestamp of the last block
    pub timestamp: Timestamp,
}

/// An entry in the active node set.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
pub struct ActiveNodeSetEntry {
    /// The node's address.
    pub address: Address,

    /// The number of times this node has voted in the current epoch.
    pub votes: u64,

    /// The number of times this node has been eligible to vote in the current epoch.
    pub eligible_votes: u64,

    /// The number of times this node has successfully proposed as leader in the current epoch.
    pub proposals: u64,

    /// The number of times this node has been eligible to propose as leader in the current epoch.
    pub slots: u64,
}

/// A single delegation from a particular user to a particular node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Delegation {
    /// The user delegating.
    pub delegator: Address,

    /// The node being delegated to.
    pub node: Address,

    /// Amount of stake delegated by this user to this node.
    pub amount: ESPTokenAmount,
}

/// A withdrawal of stake that is waiting to be claimed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct PendingWithdrawal {
    /// The owner of the pending stake.
    pub delegator: Address,

    /// The node which was previously delegated to, which stake is now being withdrawn.
    pub node: Address,

    /// The amount of stake pending withdrawal.
    pub amount: ESPTokenAmount,

    /// The timestamp recorded for the exit escrow time.
    ///
    /// Any attempts to withdrawal before this will fail.
    pub available_time: Timestamp,
}

/// A completed withdrawal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Withdrawal {
    /// The owner of the withdrawn stake.
    pub delegator: Address,

    /// The node which was previously delegated to, which stake is now withdrawn.
    pub node: Address,

    /// The amount of stake.
    pub amount: ESPTokenAmount,
}

/// Optional descriptive information about a node, fetched from a third-party URI.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct NodeMetadata {
    /// The URI this metadata is fetched from.
    ///
    /// This URI is registered alongside the node in the staking contraact.
    pub uri: Url,

    /// The content of the metadata.
    ///
    /// This content is fetched from a third-party URI, and thus should not be considered trusted,
    /// reliable, or deterministic. It is informational only.
    ///
    /// May be [`None`] if no (valid) content is available at the published `uri`.
    pub content: Option<NodeMetadataContent>,
}

/// Optimal serialization of a struct with optional fields.
///
/// For self-describing formats (e.g. JSON) a [`None`] optional field can be omitted entirely,
/// leading to smaller and cleaner output. For non-self-describing formats (e.g. bincode) a sentinel
/// value must still be serialized indicating a value of [`None`].
///
/// # Syntax
///
/// ```
/// #[derive(Deserialize)]
/// struct Example {
///     #[serde(default)]
///     field1: Option<i32>,
///
///     #[serde(default)]
///     field2: Option<String>,
/// }
///
/// serialize_struct_with_opt_fields!(Example {
///     field1,
///     field2,
/// })
/// ```
///
/// Optionally, fields can be renamed in the human-readable wire format using a string literal
/// followed by `@` (a la the `name @ Pattern { .. }` syntax in rust pattern matching) along with a
/// `#[serde(rename = "...")]` directive.
///
/// ```
/// #[derive(Deserialize)]
/// struct Example {
///     #[serde(default)]
///     field1: Option<i32>,
///
///     #[serde(default, rename = "another field!")]
///     field2: Option<String>,
/// }
///
/// serialize_struct_with_opt_fields!(Example {
///     field1,
///     "another field!" @ field2,
/// })
/// ```
macro_rules! serialize_struct_with_opt_fields {
    ($t:ident { $($($name:literal@)?$f:ident),+ $(,)?}) => {
        impl Serialize for $t {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeStruct;

                // Unpack struct to cause a compiler error if we don't mention every field.
                let $t { $($f),+ } = self;

                if s.is_human_readable() {
                    // For self-describing formats (e.g. JSON) only serialize fields which are not
                    // [`None`].
                    let mut n = 0;
                    $(
                        if $f.is_some() {
                            n += 1;
                        }
                    )+
                    let mut s = s.serialize_struct(stringify!($t), n)?;
                    $(
                        if let Some(value) = $f {
                            s.serialize_field(serialize_struct_with_opt_fields!(@field_name $($name@)?$f), value)?;
                        }
                    )+
                    s.end()
                } else {
                    let n = [$(stringify!($f)),+].len();
                    let mut s = s.serialize_struct(stringify!($t), n)?;
                    $(
                        s.serialize_field(serialize_struct_with_opt_fields!(@field_name $($name@)?$f), $f)?;
                    )+
                    s.end()
                }
            }
        }
    };

    // Helper patterns which allow us to pattern match on the presence or absence of the optional
    // `"name" @ field` syntax, returning the name that should be used for this field in the
    // serialized output.
    (@field_name $name:literal@$f:ident) => { $name };
    (@field_name $f:ident) => { stringify!($f) };
}

/// Optional descriptive information about a node.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct NodeMetadataContent {
    /// Human-readable name for the node.
    #[serde(default)]
    pub name: Option<String>,

    /// Longer description of the node.
    #[serde(default)]
    pub description: Option<String>,

    /// Company or individual operating the node.
    #[serde(default)]
    pub company_name: Option<String>,

    /// Website for `company_name`.
    #[serde(default)]
    pub company_website: Option<Url>,

    /// Consensus client the node is running.
    #[serde(default)]
    pub client_version: Option<String>,

    /// Icon for the node (at different resolutions and pixel aspect ratios).
    #[serde(default)]
    pub icon: Option<ImageSet>,
}

serialize_struct_with_opt_fields!(NodeMetadataContent {
    name,
    description,
    company_name,
    company_website,
    client_version,
    icon,
});

/// Different versions of the same image, at different resolutions and pixel aspect ratios.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct ImageSet {
    /// 14x14 icons at different pixel ratios.
    #[serde(rename = "14x14")]
    pub small: RatioSet,

    /// 24x24 icons at different pixel ratios.
    #[serde(rename = "24x24")]
    pub large: RatioSet,
}

/// Different versions of the same image, at different pixel aspect ratios.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct RatioSet {
    /// Image source for 1:1 pixel aspect ratio
    #[serde(rename = "@1x")]
    #[serde(default)]
    pub ratio1: Option<Url>,

    /// Image source for 2:1 pixel aspect ratio
    #[serde(rename = "@2x")]
    #[serde(default)]
    pub ratio2: Option<Url>,

    /// Image source for 3:1 pixel aspect ratio
    #[serde(rename = "@3x")]
    #[serde(default)]
    pub ratio3: Option<Url>,
}

serialize_struct_with_opt_fields!(RatioSet {
    "@1x" @ ratio1,
    "@2x" @ ratio2,
    "@3x" @ ratio3,
});

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test_log::test]
    fn test_ratio_set_json() {
        let set = RatioSet {
            ratio1: Some("http://example.com/".parse().unwrap()),
            ratio2: None,
            ratio3: None,
        };
        let json = serde_json::to_value(set.clone()).unwrap();

        // Omitted fields are omitted from the JSON.
        assert_eq!(
            json,
            json!({
                "@1x": "http://example.com/"
            })
        );

        // Check round trip.
        assert_eq!(set, serde_json::from_value(json).unwrap());
    }

    #[test_log::test]
    fn test_ratio_set_bincode() {
        let set = RatioSet {
            ratio1: Some("http://example.com/".parse().unwrap()),
            ratio2: None,
            ratio3: None,
        };
        let bytes = bincode::serialize(&set).unwrap();
        assert_eq!(set, bincode::deserialize(&bytes).unwrap());
    }

    #[test_log::test]
    fn test_node_metadata_content_json() {
        let metadata = NodeMetadataContent {
            name: Some("test".into()),
            ..Default::default()
        };
        let json = serde_json::to_value(metadata.clone()).unwrap();

        // Omitted fields are omitted from the JSON.
        assert_eq!(
            json,
            json!({
                "name": "test"
            })
        );

        // Check round trip.
        assert_eq!(metadata, serde_json::from_value(json).unwrap());
    }

    #[test_log::test]
    fn test_node_metadata_content_bincode() {
        let metadata = NodeMetadataContent {
            name: Some("test".into()),
            ..Default::default()
        };
        let bytes = bincode::serialize(&metadata).unwrap();
        assert_eq!(metadata, bincode::deserialize(&bytes).unwrap());
    }
}
