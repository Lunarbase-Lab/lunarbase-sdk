/** Typed failure from HTTP, JSON-RPC, or ABI response validation. */
export class RpcError extends Error {
  constructor(
    readonly code: "TRANSPORT" | "INVALID",
    message: string,
  ) {
    super(message);
    this.name = "RpcError";
  }
}
