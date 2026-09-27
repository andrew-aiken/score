import { create } from "zustand";
import { connect, StringCodec, jwtAuthenticator, DeliverPolicy } from "nats.ws";
import type {
  NatsConnection,
  JetStreamClient,
  ConsumerMessages,
} from "nats.ws";
import type { ConnectionStatus, NatsMessage } from "../types";
import {
  getCredentials,
  clearCredentials,
  isTokenExpired,
  getTeamIdFromJwt,
} from "./auth";
import { getConfig } from "./api";

function redirectToLogin(): void {
  window.location.href = "/login";
}

// NATS runs as a separate websocket service.
// If the server wasn't started with a public NATS URL, fall back to it shares a host with the page.
const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
const FALLBACK_NATS_URL = `${wsProtocol}//${window.location.hostname}:8080`;

// Cached across calls (e.g. reconnects) so GET /api/config is only fetched once per page load.
let natsUrlPromise: Promise<string> | null = null;
function resolveNatsUrl(): Promise<string> {
  if (!natsUrlPromise) {
    natsUrlPromise = getConfig()
      .then((config) => config.natsUrl || FALLBACK_NATS_URL)
      .catch(() => FALLBACK_NATS_URL);
  }
  return natsUrlPromise;
}

const sc = StringCodec();

interface NatsState {
  status: ConnectionStatus;
  messages: NatsMessage[];
  error: string | null;
  subscribedSubject: string | null;
  connect: () => Promise<void>;
  disconnect: () => Promise<void>;
  subscribeToStream: (
    subject: string,
    opts?: { deliverPolicy?: DeliverPolicy },
  ) => Promise<void>;
  clearMessages: () => void;
  getSubscribedSubjects: () => string[];
  getKvValue: (bucket: string, key: string) => Promise<string | null>;
  putKvValue: (bucket: string, key: string, value: string) => Promise<void>;
}

// Store connection references outside of Zustand state (non-serializable)
let connection: NatsConnection | null = null;
let jetstream: JetStreamClient | null = null;
let consumerMessages: ConsumerMessages | null = null;
// JWT the live connection was authenticated with, so we can detect a user switch
let connectedJwt: string | null = null;

export const useNatsStore = create<NatsState>((set, get) => ({
  status: "disconnected",
  messages: [],
  error: null,
  subscribedSubject: null,

  connect: async () => {
    // Get credentials from auth service
    const creds = getCredentials();
    if (!creds) {
      console.log("No credentials found, redirecting to login");
      redirectToLogin();
      return;
    }

    if (connection && connectedJwt === creds.jwt) {
      return;
    }

    if (connection) {
      // A different user has logged in since this connection was opened
      console.log("Credentials changed, reconnecting to NATS");
      await get().disconnect();
    }

    set({ status: "connecting", error: null, messages: [] });

    // Check if JWT is expired
    if (isTokenExpired(creds.jwt)) {
      console.log("JWT expired, redirecting to login");
      clearCredentials();
      redirectToLogin();
      return;
    }

    // Get team ID from JWT
    const teamId = getTeamIdFromJwt(creds.jwt);
    if (!teamId) {
      console.log("No team ID in JWT, redirecting to login");
      clearCredentials();
      redirectToLogin();
      return;
    }
    // The "observer" role isn't a real team - it's granted read access to
    // every team's results, so its subject must be the wildcard rather than
    // the literal (and non-existent) "results.observer.>".
    const subject = teamId === "observer" ? "results.>" : `results.${teamId}.>`;

    try {
      const encoder = new TextEncoder();
      const natsUrl = await resolveNatsUrl();
      connection = await connect({
        authenticator: jwtAuthenticator(creds.jwt, encoder.encode(creds.seed)),
        inboxPrefix: "_INBOX." + teamId + "." + crypto.randomUUID(),
        servers: natsUrl,
      });
      connectedJwt = creds.jwt;

      set({ status: "connected" });
      console.log("Connected to NATS");

      // Get JetStream context
      jetstream = connection.jetstream();
      console.log("JetStream context created");

      // Monitor connection status
      const done = connection.closed();
      done.then((err) => {
        set({ status: "disconnected" });
        connection = null;
        jetstream = null;
        connectedJwt = null;
        console.log("NATS connection closed");

        // If closed due to auth error, clear credentials and redirect to login
        if (
          err &&
          (err.message?.includes("authorization") ||
            err.message?.includes("auth"))
        ) {
          console.log(
            "Connection closed due to auth error, redirecting to login",
          );
          clearCredentials();
          redirectToLogin();
        }
      });

      // Auto-subscribe to stream with history
      await get().subscribeToStream(subject);
    } catch (err) {
      const errorMessage =
        err instanceof Error ? err.message : "Failed to connect";

      // Check if this is an auth error
      if (
        errorMessage.includes("authorization") ||
        errorMessage.includes("auth")
      ) {
        console.log(
          "Auth error during connect, clearing credentials and redirecting to login",
        );
        clearCredentials();
        redirectToLogin();
        return;
      }

      set({
        status: "error",
        error: errorMessage,
      });
      console.error("Failed to connect to NATS:", err);
      throw err;
    }
  },

  disconnect: async () => {
    if (!connection) return;

    // Stop consuming messages from JetStream
    if (consumerMessages) {
      consumerMessages.stop();
      consumerMessages = null;
      set({ subscribedSubject: null });
    }

    await connection.drain();
    connection = null;
    jetstream = null;
    connectedJwt = null;
    set({ status: "disconnected" });
  },

  subscribeToStream: async (
    subject: string,
    opts?: { deliverPolicy?: DeliverPolicy },
  ) => {
    if (!connection || !jetstream) {
      throw new Error("Not connected to NATS");
    }

    if (consumerMessages) {
      console.log("Already subscribed, stopping first...");
      consumerMessages.stop();
    }

    try {
      const deliverPolicy = opts?.deliverPolicy;
      const replayLabel =
        deliverPolicy === DeliverPolicy.LastPerSubject
          ? "last message per subject only"
          : deliverPolicy === DeliverPolicy.New
            ? "new messages only"
            : deliverPolicy === DeliverPolicy.Last
              ? "from last stream message"
              : "with history";
      console.log(`Subscribing to JetStream: ${subject} (${replayLabel})`);

      // Find the stream that contains this subject
      const jsm = await connection.jetstreamManager();
      const streamName = await jsm.streams.find(subject);

      // Ordered consumer: use a string filter (filter_subject) so the client uses the
      // new CONSUMER.CREATE API ($JS.API.CONSUMER.CREATE.<stream>.<name>.<filter>), which
      // matches team JWT pub allow ($JS.API.CONSUMER.CREATE.results.*.results.<team>.>).
      // An array here becomes filter_subjects and disables that API, publishing only to
      // $JS.API.CONSUMER.CREATE.<stream> and causing a permissions violation.
      const consumer = await jetstream.consumers.get(streamName, {
        filterSubjects: subject,
        ...(deliverPolicy !== undefined
          ? { deliver_policy: deliverPolicy }
          : {}),
      });

      // Start consuming messages
      consumerMessages = await consumer.consume();
      set({ subscribedSubject: subject });

      console.log(`Subscribed to ${subject} - consuming...`);

      // Process messages from the consumer (replay depends on deliver_policy)
      const messages = consumerMessages;
      (async () => {
        for await (const msg of messages) {
          try {
            const payload = sc.decode(msg.data);
            // Get message timestamp from JetStream metadata if available
            const timestamp = msg.info.timestampNanos
              ? new Date(Number(msg.info.timestampNanos) / 1_000_000)
              : new Date();

            console.log(
              `[JetStream] ${msg.subject} (seq: ${msg.seq}):`,
              payload,
            );

            const natsMessage: NatsMessage = {
              id: `${msg.seq || crypto.randomUUID()}`,
              subject: msg.subject,
              payload,
              timestamp,
              sequence: msg.seq,
            };

            // Add messages - deduplicate by ID to prevent duplicates
            set((state) => {
              // Check if message already exists
              if (state.messages.some((m) => m.id === natsMessage.id)) {
                return state;
              }
              return {
                messages: [...state.messages, natsMessage].slice(-501), // TODO: Set the limit on how many results that can be shown
              };
            });
          } catch (err) {
            console.error("[JetStream] Error processing message:", err);
          }
        }
      })();
    } catch (err) {
      console.error("Failed to subscribe to JetStream:", err);
      set({
        error: err instanceof Error ? err.message : "Failed to subscribe",
      });
      throw err;
    }
  },

  clearMessages: () => {
    set({ messages: [] });
  },

  getSubscribedSubjects: () => {
    const { subscribedSubject } = get();
    return subscribedSubject ? [subscribedSubject] : [];
  },

  getKvValue: async (bucket: string, key: string): Promise<string | null> => {
    if (!jetstream) {
      throw new Error("Not connected to NATS");
    }

    try {
      const kv = await jetstream.views.kv(bucket);
      const entry = await kv.get(key);

      if (entry && entry.value) {
        return sc.decode(entry.value);
      }
      return null;
    } catch (err) {
      console.error(`Failed to get KV value ${bucket}/${key}:`, err);
      throw err;
    }
  },

  putKvValue: async (
    bucket: string,
    key: string,
    value: string,
  ): Promise<void> => {
    if (!jetstream) {
      throw new Error("Not connected to NATS");
    }

    try {
      const kv = await jetstream.views.kv(bucket);
      await kv.put(key, sc.encode(value));
      console.log(`Updated KV ${bucket}/${key}`);
    } catch (err) {
      console.error(`Failed to put KV value ${bucket}/${key}:`, err);
      throw err;
    }
  },
}));
