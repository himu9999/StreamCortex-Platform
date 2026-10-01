import amqp from 'amqplib';

const QUEUE_NAME = 'video_events';
const BATCH_SIZE = 5; // We will use 5 events to trigger the AI for faster testing

// This Map acts as our "pigeonholes", sorting raw events by userId
const userBatches = new Map<string, any[]>();

// The simulated AI Intelligence Layer (hoichoi Cortex)
const processBatchWithAI = async (userId: string, events: any[]) => {
    console.log(`\n🧠 [Cortex AI] Analyzing ${events.length} events for User: ${userId}...`);
    
    // In a production build, you would pass the 'events' array to the Anthropic or OpenAI API here.
    // We are simulating the LLM response below so you can test immediately without API keys.
    const mockPrompt = `Analyze these watch events: ${JSON.stringify(events)}. Generate a 1-sentence viewer persona and a subscription upgrade recommendation.`;
    
    const mockLlmResponse = {
        persona: "Binge-watcher of suspense thrillers who frequently pauses during tense moments.",
        recommendedUpgrade: "Premium Ad-Free Plan for uninterrupted viewing."
    };

    console.log(`🎯 [Result] Persona: ${mockLlmResponse.persona}`);
    console.log(`💡 [Action] Suggesting: ${mockLlmResponse.recommendedUpgrade}\n`);
    
    // Here, you would normally save this insight back into PostgreSQL.
};

const startConsumer = async () => {
    const amqpUrl = process.env.RABBITMQ_URL || 'amqp://cortex_admin:cortex_secret@localhost:5672';
    
    try {
        const connection = await amqp.connect(amqpUrl);
        const channel = await connection.createChannel();

        // Ensure the queue exists before listening
        await channel.assertQueue(QUEUE_NAME, { durable: true });
        
        console.log(`🎧 Listening for messages in ${QUEUE_NAME}. To exit press CTRL+C`);

        // Start consuming the queue
        channel.consume(QUEUE_NAME, async (msg) => {
			if (msg !== null) {
				try {
					// Attempt to parse the incoming message
					const event = JSON.parse(msg.content.toString());
					const userId = event.userId;

					if (!userId) throw new Error("Missing userId in payload");

					console.log(`📥 Received event: ${event.action} for video ${event.videoId}`);

					if (!userBatches.has(userId)) {
						userBatches.set(userId, []);
					}
					const userBatch = userBatches.get(userId)!;
					userBatch.push(event);

					// Acknowledge successful processing
					channel.ack(msg);

					if (userBatch.length >= BATCH_SIZE) {
						await processBatchWithAI(userId, userBatch);
						userBatches.set(userId, []); 
					}
				} catch (error) {
					console.error(`⚠️ [DLQ Alert] Failed to process message. Rejecting. Error:`, error);

					// Reject the message so it doesn't get stuck in a loop. 
					// The 'false, false' means don't requeue it. In a full production setup, this sends it to a Dead Letter Queue.
					channel.nack(msg, false, false);
				}
			}
		});
    } catch (error) {
        console.error('❌ Failed to start consumer:', error);
    }
};

startConsumer();