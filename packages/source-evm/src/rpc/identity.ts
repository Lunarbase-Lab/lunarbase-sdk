/** Cancellation-safe serialized cache for one HTTP endpoint's chain identity. */
export class ChainVerification {
  private verified = false;
  private pendingVerifications = 0;
  private tail: Promise<void> = Promise.resolve();

  verify(check: () => Promise<void>): Promise<void> {
    this.pendingVerifications += 1;
    this.verified = false;
    return this.exclusive(async () => {
      try {
        await check();
        this.verified = true;
      } finally {
        this.pendingVerifications -= 1;
        if (this.pendingVerifications > 0) this.verified = false;
      }
    });
  }

  ensure(check: () => Promise<void>): Promise<void> {
    if (this.pendingVerifications === 0 && this.verified) return Promise.resolve();
    return this.exclusive(async () => {
      if (this.pendingVerifications === 0 && this.verified) return;
      await check();
      this.verified = this.pendingVerifications === 0;
    });
  }

  private exclusive(operation: () => Promise<void>): Promise<void> {
    const result = this.tail.then(operation, operation);
    this.tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}
