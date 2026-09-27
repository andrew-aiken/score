// Base URL for /api and /auth calls. The Go server serves the frontend, /api,
// and /auth from the same origin, so this defaults to the page's own origin.
// Override with VITE_API_BASE_URL (e.g. in .env.local) to point at a
// different backend during local development.
export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? window.location.origin
