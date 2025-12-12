use redis_module::{
    redis_module, Context, NextArg, RedisError, RedisResult, RedisString, RedisValue,
};
use redis_module::CommandFilterContext;

static mut COMMAND_FILTER: Option<*mut redis_module::raw::RedisModuleCommandFilter> = None;

fn command_filter_callback(fctx: &CommandFilterContext) {
    // Get the number of arguments
    let argc = fctx.args_count();
    
    if argc > 0 {
        // Get the command name (first argument)
        if let Some(cmd) = fctx.arg_get(0) {
            if let Ok(cmd_str) = cmd.try_as_str() {
                // Example: Log all SET commands
                if cmd_str.eq_ignore_ascii_case("set") {
                    // You can inspect or modify arguments here
                    // For example, you could replace sensitive data
                    
                    // Note: In a real implementation, you would use the Context
                    // to log, but we don't have access to it in the filter callback
                }
            }
        }
    }
}

fn filter_register(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    unsafe {
        if COMMAND_FILTER.is_some() {
            return Err(RedisError::String("Filter already registered".to_string()));
        }
        
        let filter = ctx.register_command_filter(command_filter_callback, 0);
        COMMAND_FILTER = Some(filter);
    }
    
    Ok(RedisValue::SimpleStringStatic("OK"))
}

fn filter_unregister(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    unsafe {
        if let Some(filter) = COMMAND_FILTER {
            ctx.unregister_command_filter(filter)?;
            COMMAND_FILTER = None;
            Ok(RedisValue::SimpleStringStatic("OK"))
        } else {
            Err(RedisError::String("No filter registered".to_string()))
        }
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

fn filter_inspect(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    // Register a temporary filter that inspects and modifies arguments
    let filter = ctx.register_command_filter(
        |fctx: &CommandFilterContext| {
            let argc = fctx.args_count();
            
            // Example: Intercept GET commands and log the client ID
            if argc > 0 {
                if let Some(cmd) = fctx.arg_get(0) {
                    if let Ok(cmd_str) = cmd.try_as_str() {
                        if cmd_str.eq_ignore_ascii_case("get") {
                            let client_id = fctx.get_client_id();
                            // In a real implementation, you might log this
                            // or store it for later use
                            let _ = client_id;
                        }
                    }
                }
            }
        },
        0,
    );
    
    // Execute a GET command which will be intercepted
    if args.len() > 1 {
        let _ = ctx.call("GET", &[&args[1]]);
    }
    
    // Unregister the filter
    ctx.unregister_command_filter(filter)?;
    
    Ok(RedisValue::SimpleStringStatic("OK"))
}

fn filter_modify(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    // Example showing argument modification
    let filter = ctx.register_command_filter(
        |fctx: &CommandFilterContext| {
            let argc = fctx.args_count();
            
            // Example: Intercept SET commands and append a prefix to the key
            if argc >= 2 {
                if let Some(cmd) = fctx.arg_get(0) {
                    if let Ok(cmd_str) = cmd.try_as_str() {
                        if cmd_str.eq_ignore_ascii_case("set") {
                            if let Some(key) = fctx.arg_get(1) {
                                if let Ok(key_str) = key.try_as_str() {
                                    // Create a new key with prefix
                                    let new_key_str = format!("filtered:{}", key_str);
                                    
                                    // Note: We would need a way to create a RedisString
                                    // without a Context here, which is a limitation
                                    // of the current API design
                                    let _ = new_key_str;
                                }
                            }
                        }
                    }
                }
            }
        },
        0,
    );
    
    // Execute a command
    if args.len() > 2 {
        let _ = ctx.call("SET", &[&args[1], &args[2]]);
    }
    
    // Unregister the filter
    ctx.unregister_command_filter(filter)?;
    
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
        ["filter.inspect", filter_inspect, "", 0, 0, 0, ""],
        ["filter.modify", filter_modify, "", 0, 0, 0, ""],
    ],
}
