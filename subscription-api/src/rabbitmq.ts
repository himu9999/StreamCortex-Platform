import amqp, { Connection, Channel } from 'amqplib';

let connection: Connection | null = null;
let channel: Channel | null = null;

const QUEUE_NAME = 'video_events';

// 1. Establish the connection to RabbitMQ
export const connectRabbitMQ = async () => {
    try {
        // Connect using the credentials defined in our docker-compose.yml
        const amqpUrl = process.env.RABBITMQ_URL || 'amqp://cortex_admin:cortex_secret@localhost:5672';
        connection = await amqp.connect(amqpUrl);
        channel = await connection.createChannel();
        
        // Ensure the queue exists before we try to send messages to it
        await channel.assertQueue(QUEUE_NAME, {
            durable: true // 'durable: true' means messages survive if RabbitMQ restarts
        });
        
        console.log('✅ Connected to RabbitMQ successfully');
    } catch (error) {
        console.error('❌ Failed to connect to RabbitMQ:', error);
        process.exit(1); // Crash the server if it can't reach the message broker
    }
};

// 2. Utility function to publish messages
export const publishEvent = async (eventData: any) => {
    if (!channel) throw new Error('RabbitMQ channel not initialized');
    
    // Buffer.from converts our JSON object into a byte stream that RabbitMQ can transport
    channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(eventData)));
};