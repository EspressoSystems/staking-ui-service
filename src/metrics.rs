//! Prometheus metrics for the staking UI service.

use std::time::{SystemTime, UNIX_EPOCH};

use prometheus::{Encoder, Gauge, IntGaugeVec, Opts, Registry, TextEncoder};

/// Current Unix time in seconds, as a float for use directly in gauges.
pub fn unix_now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs_f64()
}

/// Prometheus metrics for the staking UI service.
#[derive(Clone, Debug)]
pub struct PrometheusMetrics {
    registry: Registry,

    // L1 metrics
    /// Number of unique wallets in the latest L1 snapshot.
    pub unique_wallets: Gauge,
    /// The latest L1 block number
    pub latest_l1_block: Gauge,
    /// The finalized L1 block number.
    pub finalized_l1_block: Gauge,
    /// Number of nodes in the latest L1 snapshot.
    pub node_count: Gauge,
    pub l1_last_update_timestamp_seconds: Gauge,
    pub l1_block_timestamp_seconds: Gauge,
    pub l1_seconds_since_update: Gauge,
    pub l1_block_age_seconds: Gauge,

    // Espresso metrics
    /// Latest Espresso block number.
    pub latest_espresso_block: Gauge,
    /// Current epoch number.
    pub current_epoch: Gauge,
    /// Number of active validators in current epoch.
    pub active_validators: Gauge,
    pub espresso_last_update_timestamp_seconds: Gauge,
    pub espresso_block_timestamp_seconds: Gauge,
    pub espresso_seconds_since_update: Gauge,
    pub espresso_block_age_seconds: Gauge,
}

impl Default for PrometheusMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a gauge, register it with `registry`, and return it.
fn register_gauge(registry: &Registry, name: &str, help: &str) -> Gauge {
    let gauge = Gauge::with_opts(Opts::new(name, help))
        .unwrap_or_else(|err| panic!("failed to create {name} gauge: {err}"));
    registry
        .register(Box::new(gauge.clone()))
        .unwrap_or_else(|err| panic!("failed to register {name} gauge: {err}"));
    gauge
}

impl PrometheusMetrics {
    /// Create a new metrics instance with all gauges registered.
    pub fn new() -> Self {
        let registry = Registry::new();

        // L1 metrics
        let unique_wallets = register_gauge(
            &registry,
            "unique_wallets",
            "Number of unique wallets in the latest L1 snapshot",
        );
        let latest_l1_block = register_gauge(
            &registry,
            "latest_l1_block",
            "The latest L1 block number that has been processed",
        );
        let finalized_l1_block = register_gauge(
            &registry,
            "finalized_l1_block",
            "The finalized L1 block number",
        );
        let node_count = register_gauge(
            &registry,
            "node_count",
            "Number of validators/nodes in the latest L1 snapshot",
        );
        let l1_last_update_timestamp_seconds = register_gauge(
            &registry,
            "l1_last_update_timestamp_seconds",
            "Unix timestamp (seconds) when the L1 block was last processed. Set at startup from \
             the stored snapshot, then on each block.",
        );
        let l1_block_timestamp_seconds = register_gauge(
            &registry,
            "l1_block_timestamp_seconds",
            "Unix timestamp (seconds) of the latest processed L1 block, from the L1 block \
             header. Set at startup from the stored snapshot, then on each block.",
        );
        let l1_seconds_since_update = register_gauge(
            &registry,
            "l1_seconds_since_update",
            "Seconds since the L1 block was last processed, computed at scrape time. Set at \
             startup from the stored snapshot, then on each block.",
        );
        let l1_block_age_seconds = register_gauge(
            &registry,
            "l1_block_age_seconds",
            "Age in seconds of the latest processed L1 block, computed at scrape time. Set at \
             startup from the stored snapshot, then on each block.",
        );

        // Espresso metrics
        let latest_espresso_block = register_gauge(
            &registry,
            "latest_espresso_block",
            "Latest Espresso block number processed",
        );
        let current_epoch = register_gauge(&registry, "current_epoch", "Current epoch number");
        let active_validators = register_gauge(
            &registry,
            "active_validators",
            "Number of active validators in current epoch",
        );
        let espresso_last_update_timestamp_seconds = register_gauge(
            &registry,
            "espresso_last_update_timestamp_seconds",
            "Unix timestamp (seconds) when the latest Espresso block was processed. NaN until \
             the first Espresso block has been processed after startup.",
        );
        let espresso_block_timestamp_seconds = register_gauge(
            &registry,
            "espresso_block_timestamp_seconds",
            "Unix timestamp (seconds) of the latest processed Espresso block, from the leaf \
             header. NaN until the first Espresso block has been processed after startup.",
        );
        let espresso_seconds_since_update = register_gauge(
            &registry,
            "espresso_seconds_since_update",
            "Seconds since the latest Espresso block was processed, computed at scrape time. \
             NaN until the first Espresso block has been processed after startup.",
        );
        let espresso_block_age_seconds = register_gauge(
            &registry,
            "espresso_block_age_seconds",
            "Age in seconds of the latest processed Espresso block, computed at scrape time. \
             NaN until the first Espresso block has been processed after startup.",
        );

        // NaN rather than 0 (1970), so age gauges do not report a huge stall before the first block.
        l1_last_update_timestamp_seconds.set(f64::NAN);
        l1_block_timestamp_seconds.set(f64::NAN);
        espresso_last_update_timestamp_seconds.set(f64::NAN);
        espresso_block_timestamp_seconds.set(f64::NAN);

        Self {
            registry,
            unique_wallets,
            latest_l1_block,
            finalized_l1_block,
            node_count,
            l1_last_update_timestamp_seconds,
            l1_block_timestamp_seconds,
            l1_seconds_since_update,
            l1_block_age_seconds,
            latest_espresso_block,
            current_epoch,
            active_validators,
            espresso_last_update_timestamp_seconds,
            espresso_block_timestamp_seconds,
            espresso_seconds_since_update,
            espresso_block_age_seconds,
        }
    }

    /// Expose version information via metrics.
    pub fn register_version_info(&self) -> prometheus::Result<()> {
        let version_info = IntGaugeVec::new(
            Opts::new("version", "The version of this binary"),
            &["rev", "desc", "timestamp"],
        )?;
        self.registry.register(Box::new(version_info.clone()))?;
        version_info
            .get_metric_with_label_values(&[
                env!("VERGEN_GIT_SHA"),
                env!("VERGEN_GIT_DESCRIBE"),
                env!("VERGEN_GIT_COMMIT_TIMESTAMP"),
            ])?
            .set(1);
        Ok(())
    }

    /// Recompute the `*_seconds_since_update` and `*_block_age_seconds` gauges relative to now.
    ///
    /// These are derived, rather than tracked directly, because Datadog (which scrapes this
    /// Prometheus endpoint) has no `time()` function: ages must be computed at scrape time.
    fn update_age_gauges(&self) {
        let now = unix_now_secs();

        // NaN propagates through subtraction, so an unset raw timestamp (NaN) yields a NaN age.
        self.l1_seconds_since_update
            .set(now - self.l1_last_update_timestamp_seconds.get());
        self.l1_block_age_seconds
            .set(now - self.l1_block_timestamp_seconds.get());
        self.espresso_seconds_since_update
            .set(now - self.espresso_last_update_timestamp_seconds.get());
        self.espresso_block_age_seconds
            .set(now - self.espresso_block_timestamp_seconds.get());
    }
}

impl tide_disco::metrics::Metrics for PrometheusMetrics {
    type Error = prometheus::Error;

    fn export(&self) -> Result<String, Self::Error> {
        self.update_age_gauges();

        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer)?;
        String::from_utf8(buffer).map_err(|err| {
            prometheus::Error::Msg(format!("metrics output is not valid UTF-8: {err}"))
        })
    }
}

#[cfg(test)]
mod test {
    use tide_disco::metrics::Metrics;

    use super::{PrometheusMetrics, unix_now_secs};

    #[test]
    fn export_reports_nan_before_any_block_processed() {
        let metrics = PrometheusMetrics::new();
        let output = metrics.export().expect("export should succeed");

        for name in [
            "l1_seconds_since_update",
            "l1_block_age_seconds",
            "espresso_seconds_since_update",
            "espresso_block_age_seconds",
        ] {
            assert!(output.contains(&format!("{name} NaN")), "{output}");
        }
    }

    #[test]
    fn export_computes_age_from_recorded_timestamp() {
        let metrics = PrometheusMetrics::new();
        let now = unix_now_secs();
        metrics.l1_last_update_timestamp_seconds.set(now - 5.0);
        metrics.l1_block_timestamp_seconds.set(now - 42.0);
        metrics
            .espresso_last_update_timestamp_seconds
            .set(now - 7.0);
        metrics.espresso_block_timestamp_seconds.set(now - 90.0);

        metrics.export().expect("export should succeed");

        let l1_since_update = metrics.l1_seconds_since_update.get();
        let l1_block_age = metrics.l1_block_age_seconds.get();
        let espresso_since_update = metrics.espresso_seconds_since_update.get();
        let espresso_block_age = metrics.espresso_block_age_seconds.get();
        assert!((4.0..6.0).contains(&l1_since_update), "{l1_since_update}");
        assert!((41.0..43.0).contains(&l1_block_age), "{l1_block_age}");
        assert!(
            (6.0..8.0).contains(&espresso_since_update),
            "{espresso_since_update}"
        );
        assert!(
            (89.0..91.0).contains(&espresso_block_age),
            "{espresso_block_age}"
        );
    }
}
