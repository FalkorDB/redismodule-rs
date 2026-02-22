use redis_module::{redis_module, Context, RedisError, RedisResult, RedisString, RedisValue};
use std::os::raw::{c_int, c_void};
use std::thread;
use std::time::Duration;

// Fork done callback handler
extern "C" fn fork_done_handler(exitcode: c_int, bysignal: c_int, _user_data: *mut c_void) {
    // Note: This is called in the parent process when the child exits
    // In a real application, you might want to log this or take action based on exit code
    eprintln!(
        "Child process exited with code: {}, by signal: {}",
        exitcode, bysignal
    );
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

    // Fork a child process
    let child_pid = ctx.fork(Some(fork_done_handler), std::ptr::null_mut());

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
        redis_module::raw::Status::Ok => {
            Ok(RedisValue::SimpleStringStatic("OK"))
        }
        redis_module::raw::Status::Err => {
            Err(RedisError::String(format!(
                "Failed to kill child process with PID: {}",
                child_pid
            )))
        }
    }
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
    ],
}
