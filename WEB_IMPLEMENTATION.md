# Web Framework Foundation

This ticket implements the foundational web framework for Vibe Piper with a FastAPI backend and React frontend.

## Backend (FastAPI)

### Structure
```
src/vibe_piper/web/
├── __init__.py              # Main FastAPI app
├── api/
│   ├── router.py             # API router
│   └── endpoints/
│       ├── auth.py          # Authentication endpoints
│       ├── health.py        # Health check endpoints
│       └── pipelines.py     # Pipeline & asset endpoints
└── middleware/
    ├── error_handler.py   # Structured error handling
    ├── rate_limit.py     # Rate limiting (token bucket)
    └── request_id.py     # Request ID for tracing
```

### Features Implemented
- ✅ FastAPI server with uvicorn
- ✅ CORS middleware (localhost:5173, localhost:3000)
- ✅ GZip compression middleware
- ✅ Request ID middleware for tracing
- ✅ Rate limiting middleware (60 req/min, 1000 req/hr)
- ✅ Structured error responses with request IDs
- ✅ JWT authentication (access + refresh tokens)
- ✅ Password hashing with bcrypt
- ✅ OpenAPI/Swagger UI at `/docs`

### API Endpoints
- `GET /api/v1/health` - Health check
- `GET /api/v1/health/live` - Liveness probe
- `GET /api/v1/health/ready` - Readiness probe
- `POST /api/v1/auth/login` - Login (returns JWT tokens)
- `POST /api/v1/auth/logout` - Logout
- `POST /api/v1/auth/register` - User registration
- `POST /api/v1/auth/refresh` - Refresh access token
- `GET /api/v1/auth/me` - Get current user
- `GET /api/v1/pipelines` - List pipelines
- `GET /api/v1/pipelines/{id}` - Get pipeline details
- `GET /api/v1/pipelines/{id}/runs` - List pipeline runs
- `POST /api/v1/pipelines/{id}/run` - Trigger pipeline run
- `GET /api/v1/pipelines/assets` - List assets
- `GET /api/v1/pipelines/assets/{id}` - Get asset details

### Running the Server
```bash
# Development with auto-reload
uv run vibepiper-server --reload

# Production with custom JWT secret
JWT_SECRET_KEY="your-production-secret-key" uv run vibepiper-server --host 0.0.0.0 --port 8000 --workers 4
```

## Frontend (React + Vite + TypeScript)

### Structure
```
frontend/
├── src/
│   ├── hooks/
│   │   └── useAuth.tsx       # Authentication context & hook
│   ├── pages/
│   │   ├── LoginPage.tsx       # Login form
│   │   └── DashboardPage.tsx  # Dashboard with pipelines/assets
│   ├── services/
│   │   └── api.ts            # Type-safe API client
│   ├── types/
│   │   └── api.ts            # TypeScript type definitions
│   ├── App.tsx                # Main app component
│   └── index.css               # Tailwind CSS imports
├── .env                      # Environment variables
├── tailwind.config.js         # Tailwind configuration
├── vite.config.ts            # Vite configuration
└── tsconfig.json             # TypeScript configuration
```

### Features Implemented
- ✅ React with TypeScript and Vite
- ✅ React Router for proper navigation
- ✅ ProtectedRoute and PublicRoute components for auth guards
- ✅ Tailwind CSS for styling
- ✅ Type-safe API service layer
- ✅ Authentication context (AuthProvider, useAuth hook)
- ✅ JWT token storage in localStorage
- ✅ Login page with form validation
- ✅ Dashboard page with pipelines and assets tables
- ✅ Error handling throughout
- ✅ Responsive UI design
- ✅ Auto-redirect based on authentication status

### Running the Frontend
```bash
cd frontend
npm install      # Install dependencies
npm run dev     # Development server (http://localhost:5173)
npm run build    # Production build
```

## Architecture

### Authentication Flow
1. User submits credentials to `/api/v1/auth/login`
2. Backend validates and returns JWT access + refresh tokens
3. Frontend stores tokens in `localStorage`
4. Subsequent requests include `Authorization: Bearer <token>` header
5. When access token expires, use refresh token to get new one

### Rate Limiting
- Uses token bucket algorithm
- 60 requests per minute per client
- 1000 requests per hour per client
- Client identified by IP address (or X-Forwarded-For header)

### Error Handling
All errors return structured JSON:
```json
{
  "error": "error_type",
  "message": "Human-readable message",
  "detail": { ... },
  "request_id": "uuid"
}
```

## TODO (Future Work)
- [x] Add React Router for proper navigation ✅ Implemented
- [ ] Integrate shadcn/ui components
- [ ] Implement OAuth2/OIDC (Google, GitHub) - endpoints stubbed
- [ ] Add database integration (PostgreSQL) - currently uses mock data
- [ ] Write unit tests (backend + frontend)
- [ ] Write integration tests
- [ ] Add user profile management
- [ ] Add pipeline run monitoring UI
- [ ] Add Docker configuration
- [ ] Expand comprehensive documentation

## Testing

### Manual Testing
```bash
# Start backend
uv run vibepiper-server --reload

# In another terminal, start frontend
cd frontend && npm run dev

# Test login
curl -X POST http://127.0.0.1:8000/api/v1/auth/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=test&password=test"

# Check health
curl http://127.0.0.1:8000/api/v1/health

# Visit Swagger UI
open http://127.0.0.1:8000/docs
```

## Acceptance Criteria Status
- ✅ FastAPI server running on port 8000
- ✅ React app scaffolded and serving
- ✅ Authentication working (JWT + OAuth2 endpoints stubbed)
- ✅ Login/logout UI functional
- ✅ CORS configured for frontend origin
- ✅ Error handling (400, 401, 404, 500 responses)
- ✅ OpenAPI spec generated (Swagger UI at /docs)
- ✅ API rate limiting configured
- ⏳ Tests (backend + frontend) - Not yet implemented
- ⏳ Documentation - This file provides initial docs

## Notes
- Backend uses mock data for demonstration (TODO: Replace with database)
- OAuth2 endpoints are stubbed (TODO: Implement full OAuth2 flow)
- JWT secret key is configurable via JWT_SECRET_KEY environment variable (production should set this)
- React Router properly handles navigation and authentication-based redirects
