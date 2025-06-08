use std::time::Duration;

/// A simple delay utility for adding delays between operations
pub struct Delay {
    duration: Duration,
}

impl Delay {
    /// Create a new delay with the specified duration in milliseconds
    pub fn new(millis: u64) -> Self {
        Self {
            duration: Duration::from_millis(millis),
        }
    }

    /// Sleep for the configured duration
    pub async fn sleep(&self) {
        tokio::time::sleep(self.duration).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn test_delay() {
        let delay = Delay::new(50);
        let start = Instant::now();
        delay.sleep().await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(50));
    }
}
