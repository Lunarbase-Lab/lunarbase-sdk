/** Client-level event-filter validation. */
import { IndexerError, type ContractFilter } from "../model.js";
import { quoteCriticalTopics } from "../protocol/abi.js";

/** Accepts every topic or the complete reducer topic set. */
export function validateFilterTopics(topics: ContractFilter["topics"]): void {
  if (topics.length === 0) return;
  const required = new Set(quoteCriticalTopics().map((topic) => topic.toLowerCase()));
  const normalized = topics.map((topic) => (typeof topic === "string" ? topic.toLowerCase() : ""));
  const configured = new Set(normalized);
  if (
    topics.length !== required.size ||
    configured.size !== topics.length ||
    normalized.some((topic) => !required.has(topic))
  )
    throw new IndexerError("SOURCE", "filter topics must be empty or exactly match all quote-critical Core topics");
}
