use redis_module::{
    redis_module, server_events::ForkChildSubevent, Context, RedisError, RedisResult, RedisString,
    RedisValue,
};
use redis_module_macros::fork_child_event_handler;
use std::os::raw::c_int;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;
use std::time::Duration;

// Track fork child events
static NUM_FORK_CHILD_BORN: AtomicI64 = AtomicI64::new(0);
static NUM_FORK_CHILD_DIED: AtomicI64 = AtomicI64::new(0);

// Event handler called when fork child events occur
#[fork_child_event_handler]
fn fork_child_handler(_ctx: &Context, event: ForkChildSubevent) {
    match event {
        ForkChildSubevent::Born => {
            NUM_FORK_CHILD_BORN.fetch_add(1, Ordering::SeqCst);
            eprintln!("Fork child process born");
        }
        ForkChildSubevent::Died => {
            NUM_FORK_CHILD_DIED.fetch_add(1, Ordering::SeqCst);
            eprintln!("Fork child process died");
        }
    }
}

fn fork_example(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }

    let duration_secs: i64 = args[1].parse_integer()?;

    if duration_secs < 1 || duration_secs > 60 {
        return Err(RedisError::String(
            "Duration must be between 1 and 60 seconds".to_string(),
        ));
    }

    // Fork a child process - no manual callback needed
    let child_pid = ctx.fork(None, std::ptr::null_mut());

    if child_pid == -1 {
        // Fork failed
        return Err(RedisError::String("Fork failed".to_string()));
    } else if child_pid == 0 {
        // Child process
        // Simulate some work with progress reporting
        let iterations = 10;
        for i in 0..iterations {
            thread::sleep(Duration::from_secs(duration_secs as u64 / iterations));

            // Report progress (0.0 to 1.0)
            let progress = (i + 1) as f64 / iterations as f64;
            ctx.send_child_heartbeat(progress);
        }

        // Exit from child process with success code
        ctx.exit_from_child(0);

        // This code should never be reached
        unreachable!()
    } else {
        // Parent process
        Ok(RedisValue::SimpleString(format!(
            "Forked child process with PID: {}",
            child_pid
        )))
    }
}

fn fork_kill(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }

    let child_pid: i64 = args[1].parse_integer()?;

    let status = ctx.kill_fork_child(child_pid as c_int);

    match status {
        redis_module::raw::Status::Ok => Ok(RedisValue::SimpleStringStatic("OK")),
        redis_module::raw::Status::Err => Err(RedisError::String(format!(
            "Failed to kill child process with PID: {}",
            child_pid
        ))),
    }
}

fn fork_stats(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let born = NUM_FORK_CHILD_BORN.load(Ordering::SeqCst);
    let died = NUM_FORK_CHILD_DIED.load(Ordering::SeqCst);

    Ok(RedisValue::Array(vec![
        RedisValue::SimpleString(format!("Born: {}", born)),
        RedisValue::SimpleString(format!("Died: {}", died)),
    ]))
}

//////////////////////////////////////////////////////

redis_module! {
    name: "fork",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    commands: [
        ["fork.example", fork_example, "write", 0, 0, 0, ""],
        ["fork.kill", fork_kill, "write", 0, 0, 0, ""],
        ["fork.stats", fork_stats, "readonly", 0, 0, 0, ""],
    ],
}
