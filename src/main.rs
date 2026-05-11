use borsh::BorshDeserialize;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
};
use std::sync::Arc;
use tokio::task;
use futures::stream::StreamExt;
use std::{thread, time}; 

#[derive(Debug, Clone, BorshDeserialize)]
pub struct UserCreatedEventMessage {
    pub user_id: String,
    pub user_name: String,
}

pub struct UserCreatedHandler;

impl UserCreatedHandler {
    pub async fn handle(&self, message: Box<UserCreatedEventMessage>) -> Result<(), String> {
        let ten_millis = time::Duration::from_millis(1000); 
        let now = time::Instant::now(); 
         
        // thread::sleep(ten_millis); 

        println!(
            "In Rekkin's Computer [2406420596]. Message received: {:?}",
            message
        );
        Ok(())
    }

    pub fn get_handler_action(&self) -> String {
        todo!()
    }
}

#[tokio::main]
async fn main() {
    let amqp_uri = "amqp://guest:guest@localhost:5672";

    match setup_listener(amqp_uri).await {
        Ok(_) => println!("Listener setup completed"),
        Err(e) => eprintln!("Error: {}", e),
    }
}

async fn setup_listener(amqp_uri: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Create AMQP connection
    let connection = Connection::connect(amqp_uri, ConnectionProperties::default())
        .await?;

    // Create channel
    let channel = connection.create_channel().await?;

    // Declare queue with durable=false and use_dead_letter=true
    let queue_name = "user_created";
    let queue = channel
        .queue_declare(
            queue_name,
            QueueDeclareOptions {
                durable: false,
                auto_delete: false,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    println!("Queue declared: {:?}", queue);

    // Create consumer
    let mut consumer = channel
        .basic_consume(
            queue_name,
            "user_handler",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    println!("Consumer started, waiting for messages...");

    // Handle messages
    let handler = Arc::new(UserCreatedHandler);

    while let Some(Ok(delivery)) = consumer.next().await {
        let handler_clone = Arc::clone(&handler);

        task::spawn(async move {
            match delivery.data.as_slice() {
                data => {
                    match UserCreatedEventMessage::try_from_slice(data) {
                        Ok(message) => {
                            if let Err(e) = handler_clone.handle(Box::new(message)).await {
                                eprintln!("Error handling message: {}", e);
                            }
                            // Acknowledge message
                            if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                                eprintln!("Error acknowledging message: {}", e);
                            }
                        }
                        Err(e) => {
                            eprintln!("Error deserializing message: {}", e);
                            // Negative acknowledge
                            if let Err(e) = delivery.nack(BasicNackOptions::default()).await {
                                eprintln!("Error nacking message: {}", e);
                            }
                        }
                    }
                }
            }
        });
    }

    Ok(())
}