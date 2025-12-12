use std::collections::HashMap;
use std::os::raw::c_int;
use std::sync::{Mutex, OnceLock};

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
    /// The caller must ensure that the pointer is valid.
    pub(crate) unsafe fn new(fctx: *mut raw::RedisModuleCommandFilterCtx) -> Self {
        CommandFilterContext { fctx }
    }

    /// Get the number of arguments in the filtered command.
    ///
    /// Wrapper for `RedisModule_CommandFilterArgsCount`.
    pub fn args_count(&self) -> c_int {
        unsafe { raw::RedisModule_CommandFilterArgsCount.unwrap()(self.fctx) }
    }

    /// Get the argument at the specified position.
    ///
    /// Wrapper for `RedisModule_CommandFilterArgGet`.
    ///
    /// # Arguments
    /// * `pos` - The position of the argument (0-based)
    ///
    /// # Returns
    /// The argument as a RedisString, or None if the position is out of bounds.
    pub fn arg_get(&self, pos: c_int) -> Option<RedisString> {
        unsafe {
            let ptr = raw::RedisModule_CommandFilterArgGet.unwrap()(self.fctx, pos);
            if ptr.is_null() {
                None
            } else {
                // Note: The returned string should not be retained by the module.
                // We create a RedisString wrapper but pass null for the context
                // since we don't have access to it here.
                Some(RedisString::from_redis_module_string(
                    std::ptr::null_mut(),
                    ptr,
                ))
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
pub type CommandFilterCallback = fn(&CommandFilterContext);

// Global registry to store filter callbacks
// The key is the filter pointer, the value is the callback function
static FILTER_REGISTRY: OnceLock<Mutex<HashMap<usize, CommandFilterCallback>>> = OnceLock::new();

fn get_filter_registry() -> &'static Mutex<HashMap<usize, CommandFilterCallback>> {
    FILTER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

impl Context {
    /// Register a command filter callback.
    ///
    /// Wrapper for `RedisModule_RegisterCommandFilter`.
    ///
    /// The callback will be invoked for each command executed. Note that the
    /// callback must be a function pointer (not a closure) due to limitations
    /// in the Redis Module API.
    ///
    /// # Arguments
    /// * `callback` - The callback function to be invoked for each command
    /// * `flags` - Flags for the command filter (currently unused, pass 0)
    ///
    /// # Returns
    /// A pointer to the registered command filter, which can be used to unregister it later.
    ///
    /// # Example
    /// ```no_run
    /// # use redis_module::{Context, RedisResult};
    /// # use redis_module::context::command_filter::CommandFilterContext;
    /// fn my_filter(fctx: &CommandFilterContext) {
    ///     // Filter logic here
    /// }
    ///
    /// fn my_command(ctx: &Context, _args: Vec<redis_module::RedisString>) -> RedisResult {
    ///     let filter = ctx.register_command_filter(my_filter, 0);
    ///     // ...later...
    ///     ctx.unregister_command_filter(filter)?;
    ///     Ok(().into())
    /// }
    /// ```
    pub fn register_command_filter(
        &self,
        callback: CommandFilterCallback,
        flags: c_int,
    ) -> *mut raw::RedisModuleCommandFilter {
        let filter_ptr = unsafe {
            raw::RedisModule_RegisterCommandFilter.unwrap()(
                self.ctx,
                Some(raw_filter_callback),
                flags,
            )
        };

        // Store the callback in the registry
        let mut registry = get_filter_registry().lock().unwrap();
        registry.insert(filter_ptr as usize, callback);

        filter_ptr
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
            // Remove the callback from the registry
            let mut registry = get_filter_registry().lock().unwrap();
            registry.remove(&(filter as usize));
            Ok(())
        } else {
            Err(RedisError::Str(
                "Failed to unregister command filter, filter may not exist",
            ))
        }
    }
}

extern "C" fn raw_filter_callback(fctx: *mut raw::RedisModuleCommandFilterCtx) {
    let ctx = unsafe { CommandFilterContext::new(fctx) };

    // Call all registered callbacks
    // Note: Since the C API doesn't give us a way to identify which filter this is,
    // we call all registered callbacks. This is a limitation of the current approach.
    let registry = get_filter_registry().lock().unwrap();
    for callback in registry.values() {
        callback(&ctx);
    }
}
