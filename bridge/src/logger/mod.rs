#[cfg(target_os = "android")]
mod android;
#[cfg(not(target_os = "android"))]
mod default;

#[cfg(target_os = "android")]
pub use self::android::Logger;

#[cfg(not(target_os = "android"))]
pub use self::default::Logger;

use crate::error::{Error, Result};
use ouisync_lib::StateMonitor;
use std::panic;

pub fn new(root_monitor: Option<StateMonitor>) -> Result<Logger> {
    let logger = Logger::new().map_err(Error::InitializeLogger)?;

    // Setup panic counter
    if let Some(root_monitor) = root_monitor {
        let panic_counter = root_monitor
            .make_child("Session")
            .make_value("panic_counter", 0u32);

        // Make sure this is done after creating the logger because the logger might override the panic
        // hook.
        let default_panic_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            *panic_counter.get() += 1;
            default_panic_hook(panic_info);
        }));
    }

    Ok(logger)
}