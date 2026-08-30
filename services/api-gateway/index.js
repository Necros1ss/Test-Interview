const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.API_GATEWAY_PORT || 3000;
const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL || 'http://localhost:3001';
const INVENTORY_SERVICE_URL = process.env.INVENTORY_SERVICE_URL || 'http://localhost:3003';
const ANALYTICS_SERVICE_URL = process.env.ANALYTICS_SERVICE_URL || 'http://localhost:3005';

// Auth Token Configuration
const VALID_TOKENS = new Set([
  process.env.API_AUTH_TOKEN || 'techlab-secret-token-2026',
  'techlab-test-token',
  'demo-token-123'
]);

// ----------------------------------------------------
// 1. Correlation ID Middleware
// ----------------------------------------------------
app.use((req, res, next) => {
  const correlationId = req.headers['x-correlation-id'] || `corr-${uuidv4()}`;
  req.correlationId = correlationId;
  res.setHeader('x-correlation-id', correlationId);
  next();
});

// ----------------------------------------------------
// 2. Sliding Window Rate Limiting Middleware
// ----------------------------------------------------
const RATE_LIMIT_WINDOW_MS = parseInt(process.env.RATE_LIMIT_WINDOW_MS || '60000', 10);
const RATE_LIMIT_MAX_REQUESTS = parseInt(process.env.RATE_LIMIT_MAX_REQUESTS || '2000', 10);
const requestTimestamps = new Map();

function rateLimiter(req, res, next) {
  const clientKey = req.ip || req.headers['x-forwarded-for'] || 'default-client';
  const now = Date.now();

  let timestamps = requestTimestamps.get(clientKey) || [];
  // Filter out timestamps outside the sliding window
  timestamps = timestamps.filter((ts) => now - ts < RATE_LIMIT_WINDOW_MS);

  if (timestamps.length >= RATE_LIMIT_MAX_REQUESTS) {
    const oldestTimestamp = timestamps[0];
    const retryAfterSec = Math.ceil((RATE_LIMIT_WINDOW_MS - (now - oldestTimestamp)) / 1000);
    res.setHeader('Retry-After', retryAfterSec);
    return res.status(429).json({
      error: 'Too Many Requests',
      message: `Rate limit exceeded. Maximum ${RATE_LIMIT_MAX_REQUESTS} requests per ${RATE_LIMIT_WINDOW_MS / 1000}s.`,
      retryAfterSeconds: retryAfterSec,
      correlationId: req.correlationId
    });
  }

  timestamps.push(now);
  requestTimestamps.set(clientKey, timestamps);
  next();
}

// ----------------------------------------------------
// 3. Authentication Middleware
// ----------------------------------------------------
function authenticate(req, res, next) {
  // Allow health endpoint to be public
  if (req.path === '/health' || req.path === '/api/health') {
    return next();
  }

  const authHeader = req.headers['authorization'];
  const apiKeyHeader = req.headers['x-api-key'];

  let token = null;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.substring(7).trim();
  } else if (apiKeyHeader) {
    token = apiKeyHeader.trim();
  }

  if (!token || !VALID_TOKENS.has(token)) {
    return res.status(401).json({
      error: 'Unauthorized',
      message: 'Missing or invalid authentication token. Provide Authorization: Bearer <token> or x-api-key.',
      correlationId: req.correlationId
    });
  }

  next();
}

// Apply Rate Limiting and Authentication
app.use(rateLimiter);
app.use(authenticate);

// ----------------------------------------------------
// 4. REST Proxy Routes
// ----------------------------------------------------

// Health Check
app.get(['/health', '/api/health'], async (req, res) => {
  return res.json({
    status: 'UP',
    service: 'API Gateway',
    timestamp: new Date().toISOString(),
    correlationId: req.correlationId
  });
});

// Proxy to Order Service: POST /api/orders
app.post('/api/orders', async (req, res) => {
  const queryStr = req.url.includes('?') ? req.url.substring(req.url.indexOf('?')) : '';
  const targetUrl = `${ORDER_SERVICE_URL}/orders${queryStr}`;

  try {
    const response = await fetch(targetUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-correlation-id': req.correlationId,
        ...(req.headers['x-mode'] ? { 'x-mode': req.headers['x-mode'] } : {})
      },
      body: JSON.stringify(req.body)
    });

    const data = await response.json();
    return res.status(response.status).json({ ...data, correlationId: req.correlationId });
  } catch (err) {
    console.error(`[API Gateway] Error proxying to Order Service (${targetUrl}):`, err.message);
    return res.status(502).json({
      error: 'Bad Gateway',
      message: `Failed to connect to Order Service: ${err.message}`,
      correlationId: req.correlationId
    });
  }
});

// Proxy to Order Service: GET /api/orders/:id
app.get('/api/orders/:id', async (req, res) => {
  const targetUrl = `${ORDER_SERVICE_URL}/orders/${req.params.id}`;

  try {
    const response = await fetch(targetUrl, {
      headers: {
        'x-correlation-id': req.correlationId
      }
    });

    const data = await response.json();
    return res.status(response.status).json(data);
  } catch (err) {
    return res.status(502).json({
      error: 'Bad Gateway',
      message: `Failed to connect to Order Service: ${err.message}`,
      correlationId: req.correlationId
    });
  }
});

// Proxy to Order Service: GET /api/orders
app.get('/api/orders', async (req, res) => {
  const queryStr = req.url.includes('?') ? req.url.substring(req.url.indexOf('?')) : '';
  const targetUrl = `${ORDER_SERVICE_URL}/orders${queryStr}`;

  try {
    const response = await fetch(targetUrl, {
      headers: {
        'x-correlation-id': req.correlationId
      }
    });

    const data = await response.json();
    return res.status(response.status).json(data);
  } catch (err) {
    return res.status(502).json({
      error: 'Bad Gateway',
      message: `Failed to connect to Order Service: ${err.message}`,
      correlationId: req.correlationId
    });
  }
});

// Proxy to Inventory Service: GET /api/stock/:productId
app.get('/api/stock/:productId', async (req, res) => {
  const targetUrl = `${INVENTORY_SERVICE_URL}/stock/${req.params.productId}`;

  try {
    const response = await fetch(targetUrl, {
      headers: {
        'x-correlation-id': req.correlationId
      }
    });

    const data = await response.json();
    return res.status(response.status).json(data);
  } catch (err) {
    return res.status(502).json({
      error: 'Bad Gateway',
      message: `Failed to connect to Inventory Service: ${err.message}`,
      correlationId: req.correlationId
    });
  }
});

// Proxy to Analytics Service: GET /api/metrics
app.get('/api/metrics', async (req, res) => {
  const targetUrl = `${ANALYTICS_SERVICE_URL}/metrics`;

  try {
    const response = await fetch(targetUrl, {
      headers: {
        'x-correlation-id': req.correlationId
      }
    });

    const data = await response.json();
    return res.status(response.status).json(data);
  } catch (err) {
    return res.status(502).json({
      error: 'Bad Gateway',
      message: `Failed to connect to Analytics Service: ${err.message}`,
      correlationId: req.correlationId
    });
  }
});

// Start Server if run directly
if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`[API Gateway] Listening on port ${PORT}`);
    console.log(`[API Gateway] Authentication & Rate Limiting (max ${RATE_LIMIT_MAX_REQUESTS} req/${RATE_LIMIT_WINDOW_MS / 1000}s) active.`);
  });
}

module.exports = app;
