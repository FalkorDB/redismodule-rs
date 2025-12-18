use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::slice;
use std::sync::{Mutex, OnceLock};
use std::collections::HashMap;

use crate::raw;
use crate::{Context, RedisError};

/// Callback function type for cluster message receivers.
/// 
/// # Arguments
/// * `ctx` - The Redis module context
/// * `sender_id` - The cluster node ID of the sender
/// * `message_type` - The message type (0-255)
/// * `payload` - The message payload as a byte slice
pub type ClusterMessageCallback = fn(&Context, &str, u8, &[u8]);

impl Context {
    /// Wrapper for `RedisModule_RegisterClusterMessageReceiver`.
    ///
    /// Registers a callback function to receive cluster messages of a specific type.
    /// The callback will be invoked whenever a message of the specified type is received
    /// from another node in the Redis cluster.
    ///
    /// # Arguments
    /// * `message_type` - The message type to register for (0-255)
    /// * `callback` - The callback function to invoke when a message is received
    ///
    /// # Returns
    /// * `Ok(())` if the callback was registered successfully
    /// * `Err(RedisError)` if the callback could not be registered
    ///
    /// # Example
    /// ```ignore
    /// fn message_handler(ctx: &Context, sender_id: &str, message_type: u8, payload: &[u8]) {
    ///     ctx.log_debug(&format!("Received message from {}: {:?}", sender_id, payload));
    /// }
    ///
    /// ctx.register_cluster_message_receiver(42, message_handler)?;
    /// ```
    pub fn register_cluster_message_receiver(
        &self,
        message_type: u8,
        callback: ClusterMessageCallback,
    ) -> Result<(), RedisError> {
        // Store the callback in a global registry
        register_callback(message_type, callback)?;

        unsafe {
            raw::RedisModule_RegisterClusterMessageReceiver.unwrap()(
                self.ctx,
                message_type,
                Some(raw_cluster_message_callback),
            );
        }
        
        Ok(())
    }

    /// Wrapper for `RedisModule_SendClusterMessage`.
    ///
    /// Sends a message to a specific node or all nodes in the Redis cluster.
    ///
    /// # Arguments
    /// * `target_id` - The cluster node ID to send to, or `None` to broadcast to all nodes
    /// * `message_type` - The message type (0-255)
    /// * `message` - The message payload as a byte slice
    ///
    /// # Returns
    /// * `Ok(())` if the message was sent successfully
    /// * `Err(RedisError)` if the message could not be sent
    ///
    /// # Example
    /// ```ignore
    /// // Send to a specific node
    /// ctx.send_cluster_message(Some("node123"), 42, b"Hello, node!")?;
    ///
    /// // Broadcast to all nodes
    /// ctx.send_cluster_message(None, 42, b"Hello, everyone!")?;
    /// ```
    pub fn send_cluster_message(
        &self,
        target_id: Option<&str>,
        message_type: u8,
        message: &[u8],
    ) -> Result<(), RedisError> {
        let target_cstring;
        let target_ptr = match target_id {
            Some(id) => {
                target_cstring = CString::new(id)
                    .map_err(|_| RedisError::Str("Invalid target_id: contains null byte"))?;
                target_cstring.as_ptr()
            }
            None => std::ptr::null(),
        };

        let result = unsafe {
            raw::RedisModule_SendClusterMessage.unwrap()(
                self.ctx,
                target_ptr,
                message_type,
                message.as_ptr() as *const c_char,
                message.len() as u32,
            )
        };

        if result == raw::REDISMODULE_OK as i32 {
            Ok(())
        } else {
            Err(RedisError::Str("Failed to send cluster message"))
        }
    }
}

// Global registry for cluster message callbacks
static CLUSTER_MESSAGE_CALLBACKS: OnceLock<Mutex<HashMap<u8, ClusterMessageCallback>>> = OnceLock::new();

fn get_callbacks() -> &'static Mutex<HashMap<u8, ClusterMessageCallback>> {
    CLUSTER_MESSAGE_CALLBACKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn register_callback(message_type: u8, callback: ClusterMessageCallback) -> Result<(), RedisError> {
    let mut callbacks = get_callbacks()
        .lock()
        .map_err(|_| RedisError::Str("Failed to acquire lock on cluster message callbacks"))?;
    callbacks.insert(message_type, callback);
    Ok(())
}

extern "C" fn raw_cluster_message_callback(
    ctx: *mut raw::RedisModuleCtx,
    sender_id: *const c_char,
    message_type: u8,
    payload: *const u8,
    len: u32,
) {
    let ctx = &Context::new(ctx);

    // Convert sender_id to a Rust string
    let sender_id_str = if sender_id.is_null() {
        ""
    } else {
        unsafe {
            CStr::from_ptr(sender_id)
                .to_str()
                .unwrap_or("")
        }
    };

    // Convert payload to a byte slice
    let payload_slice = if payload.is_null() || len == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(payload, len as usize) }
    };

    // Look up the callback for this message type
    match get_callbacks().lock() {
        Ok(callbacks) => {
            if let Some(callback) = callbacks.get(&message_type) {
                callback(ctx, sender_id_str, message_type, payload_slice);
            } else {
                ctx.log_debug(&format!(
                    "No callback registered for cluster message type {}",
                    message_type
                ));
            }
        }
        Err(_) => {
            ctx.log_warning("Failed to acquire lock on cluster message callbacks");
        }
    }
}
