//! Cross-platform USB serial transport and MCU transaction primitives.
pub mod ack;
pub mod response;
pub mod retry;
pub mod retransmit;
pub mod scheduler;
pub mod clock;
pub mod config;
pub mod pin;
pub mod session;
pub use session::{IdentifyProgress, ReliableSerialSession, SessionError, SessionEvent};

pub use ack::{AckEvent, AckTracker};
pub use response::{Response, ResponseRouter, ResponseWaiter, WaitError};
pub use retry::{AckRetryDecision, RetryPolicy, RetryTransaction};
pub use retransmit::{AckResult, Retransmission, RetransmitError, RetransmitState, TrackedFrame, DEFAULT_RTO_SECONDS, DUP_ACK_THRESHOLD, MAX_RTO_SECONDS, MIN_RTO_SECONDS};
pub use scheduler::{CommandScheduler, ReceivedFrameResult, ScheduleError, ScheduledCommand, ScheduledFrame, BACKGROUND_PRIORITY_CLOCK};
pub use clock::{ClockEstimate, ClockSync, ClockSyncError, ClockUpdate, UartTiming};
pub use config::{ConfigAction, ConfigError, ConfigHandshakeStage, ConfigResponse, McuConfigHandshake, McuConfigPlan, OidAllocator};
pub use pin::{PinResolver, PinResolverError};
