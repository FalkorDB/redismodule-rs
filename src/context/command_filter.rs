use std::os::raw::c_int;

use crate::raw;
use crate::{Context, RedisError, RedisString};

/// A wrapper around the Redis Module Command Filter Context.
///
/// This context is passed to command filter callbacks and provides methods
/// to inspect and modify command arguments.
pub struct CommandFilterContext {
    fctx: *mut raw::RedisModuleCommandFilterCtx,
}

impl CommandFilterContext {
    /// Create a new CommandFilterContext from a raw pointer.
    ///
    /// # Safety
    /// The caller must ensure that the pointer is valid and only used within
    /// the lifetime of the command filter callback.
    pub unsafe fn new(fctx: *mut raw::RedisModuleCommandFilterCtx) -> Self {
        CommandFilterContext { fctx }
    }

    /// Get the number of arguments in the filtered command.
    ///
    /// Wrapper for `RedisModule_CommandFilterArgsCount`.
    pub fn args_count(&self) -> c_int {
        unsafe { raw::RedisModule_CommandFilterArgsCount.unwrap()(self.fctx) }
    }

    /// Get the argument at the specified position as a string slice.
    ///
    /// Wrapper for `RedisModule_CommandFilterArgGet`.
    ///
    /// # Arguments
    /// * `pos` - The position of the argument (0-based)
    ///
    /// # Returns
    /// The argument as a string slice, or None if the position is out of bounds
    /// or the argument is not valid UTF-8.
    ///
    /// # Note
    /// The returned string slice is only valid for the duration of the filter callback.
    /// Do not store it beyond the callback's lifetime.
    pub fn arg_get_str(&self, pos: c_int) -> Option<&str> {
        unsafe {
            let ptr = raw::RedisModule_CommandFilterArgGet.unwrap()(self.fctx, pos);
            if ptr.is_null() {
                None
            } else {
                RedisString::from_ptr(ptr).ok()
            }
        }
    }

    /// Insert an argument at the specified position.
    ///
    /// Wrapper for `RedisModule_CommandFilterArgInsert`.
    ///
    /// # Arguments
    /// * `pos` - The position where the argument should be inserted (0-based)
    /// * `arg` - The argument to insert
    ///
    /// # Returns
    /// Ok(()) on success, or an error if the operation failed.
    pub fn arg_insert(&self, pos: c_int, arg: &RedisString) -> Result<(), RedisError> {
        let status: raw::Status = unsafe {
            raw::RedisModule_CommandFilterArgInsert.unwrap()(self.fctx, pos, arg.inner)
        }
        .into();

        if status == raw::Status::Ok {
            Ok(())
        } else {
            Err(RedisError::Str("Failed to insert argument"))
        }
    }

    /// Replace the argument at the specified position.
    ///
    /// Wrapper for `RedisModule_CommandFilterArgReplace`.
    ///
    /// # Arguments
    /// * `pos` - The position of the argument to replace (0-based)
    /// * `arg` - The new argument value
    ///
    /// # Returns
    /// Ok(()) on success, or an error if the operation failed.
    pub fn arg_replace(&self, pos: c_int, arg: &RedisString) -> Result<(), RedisError> {
        let status: raw::Status = unsafe {
            raw::RedisModule_CommandFilterArgReplace.unwrap()(self.fctx, pos, arg.inner)
        }
        .into();

        if status == raw::Status::Ok {
            Ok(())
        } else {
            Err(RedisError::Str("Failed to replace argument"))
        }
    }

    /// Delete the argument at the specified position.
    ///
    /// Wrapper for `RedisModule_CommandFilterArgDelete`.
    ///
    /// # Arguments
    /// * `pos` - The position of the argument to delete (0-based)
    ///
    /// # Returns
    /// Ok(()) on success, or an error if the operation failed.
    pub fn arg_delete(&self, pos: c_int) -> Result<(), RedisError> {
        let status: raw::Status =
            unsafe { raw::RedisModule_CommandFilterArgDelete.unwrap()(self.fctx, pos) }.into();

        if status == raw::Status::Ok {
            Ok(())
        } else {
            Err(RedisError::Str("Failed to delete argument"))
        }
    }

    /// Get the client ID of the client that issued the filtered command.
    ///
    /// Wrapper for `RedisModule_CommandFilterGetClientId`.
    ///
    /// # Returns
    /// The client ID as an unsigned 64-bit integer.
    pub fn get_client_id(&self) -> u64 {
        unsafe { raw::RedisModule_CommandFilterGetClientId.unwrap()(self.fctx) }
    }
}

/// Type alias for command filter callbacks.
///
/// Note: Due to limitations in the Redis Module C API, command filters cannot receive
/// user data. Therefore, the callback must be a static function pointer, not a closure
/// that captures variables.
pub type CommandFilterCallback = unsafe extern "C" fn(*mut raw::RedisModuleCommandFilterCtx);

impl Context {
    /// Register a command filter callback.
    ///
    /// Wrapper for `RedisModule_RegisterCommandFilter`.
    ///
    /// The callback will be invoked for each command executed. Due to limitations
    /// in the Redis Module C API, the callback must be an `extern "C"` function that
    /// matches the signature expected by Redis.
    ///
    /// Typically, you would create a wrapper function that calls your Rust implementation:
    ///
    /// ```no_run
    /// # use redis_module::raw;
    /// # use redis_module::CommandFilterContext;
    /// unsafe extern "C" fn my_filter_wrapper(fctx: *mut raw::RedisModuleCommandFilterCtx) {
    ///     let filter_ctx = CommandFilterContext::new(fctx);
    ///     my_filter_impl(&filter_ctx);
    /// }
    ///
    /// fn my_filter_impl(fctx: &CommandFilterContext) {
    ///     // Your filter logic here
    /// }
    /// ```
    ///
    /// # Arguments
    /// * `callback` - The callback function to be invoked for each command
    /// * `flags` - Flags for the command filter (currently unused, pass 0)
    ///
    /// # Returns
    /// A pointer to the registered command filter, which can be used to unregister it later.
    ///
    /// # Safety
    /// The caller must ensure that the callback function is valid and properly handles
    /// the raw pointer it receives.
    pub unsafe fn register_command_filter(
        &self,
        callback: CommandFilterCallback,
        flags: c_int,
    ) -> *mut raw::RedisModuleCommandFilter {
        raw::RedisModule_RegisterCommandFilter.unwrap()(self.ctx, Some(callback), flags)
    }

    /// Unregister a previously registered command filter.
    ///
    /// Wrapper for `RedisModule_UnregisterCommandFilter`.
    ///
    /// # Arguments
    /// * `filter` - The filter pointer returned by `register_command_filter`
    ///
    /// # Returns
    /// Ok(()) on success, or an error if the operation failed.
    pub fn unregister_command_filter(
        &self,
        filter: *mut raw::RedisModuleCommandFilter,
    ) -> Result<(), RedisError> {
        let status: raw::Status =
            unsafe { raw::RedisModule_UnregisterCommandFilter.unwrap()(self.ctx, filter) }.into();

        if status == raw::Status::Ok {
            Ok(())
        } else {
            Err(RedisError::Str(
                "Failed to unregister command filter, filter may not exist",
            ))
        }
    }
}
