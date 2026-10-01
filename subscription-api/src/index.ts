import express, { Request, Response } from 'express';
import { connectRabbitMQ, publishEvent } from './rabbitmq';
import rateLimit from 'express-rate-limit';

// Create the rate limiter: Max 100 requests per 1 minute per IP
const eventLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, 
    max: 100, 
    message: { error: 'Too many events sent. Please slow down.' }
});

const app = express();
app.use(express.json()); // Middleware to parse incoming JSON

// Endpoint 1: Subscription Mock
app.post('/subscribe', (req: Request, res: Response) => {
    const { userId, plan } = req.body;
    
    // In a real system, this would insert a record into PostgreSQL via Prisma/TypeORM
    // and integrate with Stripe. For now, we mock the success.
    console.log(`User ${userId} subscribed to ${plan} plan.`);
    
    res.status(200).json({
        status: 'success',
        message: 'Subscription activated',
        userId
    });
});

// Endpoint 2: High-Throughput Event Ingestion
app.post('/events', eventLimiter, async (req: Request, res: Response) => {
    const { userId, videoId, timestamp, action, playbackPositionSeconds } = req.body;

    // Basic validation matching our API contract
    if (!userId || !videoId || !action) {
        return res.status(400).json({ error: 'Missing required fields' });
    }

    try {
        const eventPayload = { userId, videoId, timestamp, action, playbackPositionSeconds };
        
        // Push to RabbitMQ instantly
        await publishEvent(eventPayload);

        // Architectural Decision: We return '202 Accepted' instead of '201 Created' or '200 OK'.
        // 202 means "we received your request and will process it asynchronously."
        res.status(202).json({ status: 'accepted' });
    } catch (error) {
        console.error('Event ingestion error:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

const PORT = 3000;

// Start the server, but only after RabbitMQ connects
const startServer = async () => {
    await connectRabbitMQ();
    app.listen(PORT, () => {
        console.log(`🚀 API Server running on http://localhost:${PORT}`);
    });
};

startServer();