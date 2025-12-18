use redis_module::{redis_module, Context, NextArg, RedisResult, RedisString, Status};

const MESSAGE_TYPE: u8 = 42;

// Callback function to handle received cluster messages
fn handle_cluster_message(ctx: &Context, sender_id: &str, message_type: u8, payload: &[u8]) {
    let payload_str = String::from_utf8_lossy(payload);
    ctx.log_notice(&format!(
        "Received cluster message from node {}: type={}, payload='{}'",
        sender_id, message_type, payload_str
    ));
}

// Command to register a cluster message receiver
fn register_receiver(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    ctx.register_cluster_message_receiver(MESSAGE_TYPE, handle_cluster_message)?;
    Ok("OK".into())
}

// Command to send a message to a specific node
fn send_to_node(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let target_id = args.next_string()?;
    let message = args.next_string()?;

    ctx.send_cluster_message(Some(&target_id), MESSAGE_TYPE, message.as_bytes())?;
    
    Ok("Message sent".into())
}

// Command to broadcast a message to all nodes
fn broadcast(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let message = args.next_string()?;

    ctx.send_cluster_message(None, MESSAGE_TYPE, message.as_bytes())?;
    
    Ok("Broadcast sent".into())
}

//////////////////////////////////////////////////////

redis_module! {
    name: "cluster_message",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    init: register_receiver_on_load,
    commands: [
        ["cluster_msg.register", register_receiver, "", 0, 0, 0, ""],
        ["cluster_msg.send", send_to_node, "", 0, 0, 0, ""],
        ["cluster_msg.broadcast", broadcast, "", 0, 0, 0, ""],
    ],
}

fn register_receiver_on_load(ctx: &Context, _args: &[RedisString]) -> Status {
    // Automatically register the message receiver when the module loads
    match ctx.register_cluster_message_receiver(MESSAGE_TYPE, handle_cluster_message) {
        Ok(_) => {
            ctx.log_notice("Cluster message receiver registered for module initialization");
            Status::Ok
        }
        Err(e) => {
            ctx.log_warning(&format!("Failed to register cluster message receiver: {:?}", e));
            Status::Err
        }
    }
}
