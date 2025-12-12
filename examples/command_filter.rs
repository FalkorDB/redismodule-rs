use redis_module::{
    raw, redis_module, Context, NextArg, RedisError, RedisResult, RedisString, RedisValue,
};
use redis_module::CommandFilterContext;
use std::sync::atomic::{AtomicPtr, Ordering};

static COMMAND_FILTER: AtomicPtr<raw::RedisModuleCommandFilter> = AtomicPtr::new(std::ptr::null_mut());

unsafe extern "C" fn command_filter_callback(fctx: *mut raw::RedisModuleCommandFilterCtx) {
    let filter_ctx = CommandFilterContext::new(fctx);
    command_filter_impl(&filter_ctx);
}

fn command_filter_impl(fctx: &CommandFilterContext) {
    // Get the number of arguments
    let argc = fctx.args_count();
    
    if argc > 0 {
        // Get the command name (first argument)
        if let Some(cmd_str) = fctx.arg_get_str(0) {
            // Example: Log all SET commands
            if cmd_str.eq_ignore_ascii_case("set") {
                // You can inspect or modify arguments here
                // For example, you could replace sensitive data
                
                // Note: In a real implementation, you would use the Context
                // to log, but we don't have access to it in the filter callback
                let _client_id = fctx.get_client_id();
            }
        }
    }
}

fn filter_register(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let current = COMMAND_FILTER.load(Ordering::Acquire);
    
    if !current.is_null() {
        return Err(RedisError::String("Filter already registered".to_string()));
    }
    
    let filter = unsafe { ctx.register_command_filter(command_filter_callback, 0) };
    COMMAND_FILTER.store(filter, Ordering::Release);
    
    Ok(RedisValue::SimpleStringStatic("OK"))
}

fn filter_unregister(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let filter = COMMAND_FILTER.swap(std::ptr::null_mut(), Ordering::AcqRel);
    
    if !filter.is_null() {
        ctx.unregister_command_filter(filter)?;
        Ok(RedisValue::SimpleStringStatic("OK"))
    } else {
        Err(RedisError::String("No filter registered".to_string()))
    }
}

fn filter_test_args(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args_iter = args.into_iter().skip(1);
    let key = args_iter.next_arg()?;
    let value = args_iter.next_arg()?;
    
    // This SET command will be intercepted by the filter if it's registered
    ctx.call("SET", &[&key, &value])?;
    
    Ok(RedisValue::SimpleStringStatic("OK"))
}

//////////////////////////////////////////////////////

redis_module! {
    name: "command_filter",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    commands: [
        ["filter.register", filter_register, "", 0, 0, 0, ""],
        ["filter.unregister", filter_unregister, "", 0, 0, 0, ""],
        ["filter.test_args", filter_test_args, "", 0, 0, 0, ""],
    ],
}
